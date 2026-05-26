// Package control implements the bootstrap control-plane service.
package control

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
	"github.com/samber/oops"
)

type Config struct {
	Addr           string
	ClusterID      string
	BootstrapNodes []controlapi.RegisterNodeBody
	Liveness       LivenessConfig
	Migration      MigrationConfig
	Persistence    PersistenceConfig
	Raft           RaftConfig
}

type LivenessConfig struct {
	SweepInterval time.Duration
	SuspectAfter  time.Duration
	DeadAfter     time.Duration
}

type PersistenceConfig struct {
	SnapshotPath string
}

type ServiceRuntime struct {
	cfg       Config
	state     *ControlState
	fsm       *ControlFSM
	liveness  LivenessConfig
	migration MigrationConfig
	now       func() time.Time
	raftMu    sync.RWMutex
	raft      *DragonboatRuntime
}

func NewServiceRuntime(cfg Config) *ServiceRuntime {
	return NewServiceRuntimeWithEvents(cfg, nil)
}

func NewServiceRuntimeWithEvents(cfg Config, bus eventx.BusRuntime) *ServiceRuntime {
	state := NewControlStateWithEvents(cfg.ClusterID, bus)

	return &ServiceRuntime{
		cfg:       cfg,
		state:     state,
		fsm:       NewControlFSM(state),
		now:       time.Now,
		liveness:  normalizeLivenessConfig(cfg.Liveness),
		migration: cfg.Migration,
	}
}

func (s *ServiceRuntime) CreateNamespace(ctx context.Context, namespace string) (controlapi.CreateNamespaceResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandCreateNamespace, Namespace: namespace})
	return result.CreateNamespace, err
}

func (s *ServiceRuntime) CreateSpace(ctx context.Context, namespace, space string) (controlapi.CreateSpaceResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandCreateSpace, Namespace: namespace, Space: space})
	return result.CreateSpace, err
}

func (s *ServiceRuntime) CreateEntity(ctx context.Context, namespace, space, entity string) (controlapi.CreateEntityResponse, error) {
	result, err := s.apply(ctx, Command{
		Type:      CommandCreateEntity,
		Namespace: namespace,
		Space:     space,
		Entity:    entity,
	})
	return result.CreateEntity, err
}

func (s *ServiceRuntime) BumpNamespaceVersion(ctx context.Context, namespace string) (controlapi.BumpNamespaceVersionResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandBumpNamespace, Namespace: namespace})
	return result.BumpNamespace, err
}

func (s *ServiceRuntime) BumpSpaceVersion(ctx context.Context, namespace, space string) (controlapi.BumpSpaceVersionResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandBumpSpace, Namespace: namespace, Space: space})
	return result.BumpSpace, err
}

func (s *ServiceRuntime) RegisterNode(ctx context.Context, nodeID, addr string) (controlapi.RegisterNodeResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandRegisterNode, NodeID: nodeID, Addr: addr, NowUnix: s.nowUnix()})
	return result.RegisterNode, err
}

func (s *ServiceRuntime) Heartbeat(ctx context.Context, nodeID, addr string) (controlapi.HeartbeatResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandHeartbeat, NodeID: nodeID, Addr: addr, NowUnix: s.nowUnix()})
	return result.Heartbeat, err
}

func (s *ServiceRuntime) RemoveNode(ctx context.Context, nodeID string) (controlapi.RemoveNodeResponse, error) {
	result, err := s.apply(ctx, Command{Type: CommandRemoveNode, NodeID: nodeID})
	return result.RemoveNode, err
}

func (s *ServiceRuntime) Namespaces() controlapi.NamespacesBody {
	return s.state.Namespaces()
}

func (s *ServiceRuntime) Spaces() controlapi.SpacesBody {
	return s.state.Spaces()
}

func (s *ServiceRuntime) Entities() controlapi.EntitiesBody {
	return s.state.Entities()
}

func (s *ServiceRuntime) Nodes() controlapi.NodesBody {
	return s.state.Nodes()
}

func (s *ServiceRuntime) Revision() uint64 {
	return s.state.Revision()
}

func (s *ServiceRuntime) RouteCount() uint64 {
	return checkedUint64(s.state.RouteCount())
}

func (s *ServiceRuntime) MigrationPlans() controlapi.MigrationPlansBody {
	return s.state.MigrationPlans()
}

func HTTPConfig(svc *ServiceRuntime) runtime.HTTPConfig {
	cfg := svc.cfg
	return runtime.HTTPConfig{
		Name: "control",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"cluster_id": cfg.ClusterID,
			"role":       "control-plane",
		},
	}
}

func controlStateError(message string, err error) error {
	switch {
	case hasControlOopsCode(err, "namespace_not_found", "space_not_found"):
		return httpx.NewError(http.StatusNotFound, message, err)
	case hasControlOopsCode(err, "control_raft_not_available", "control_raft_operation_failed"):
		return httpx.NewError(http.StatusServiceUnavailable, message, err)
	case hasControlOopsCode(
		err,
		"invalid_node",
		"invalid_namespace",
		"invalid_space",
		"invalid_entity",
		"control_raft_node_invalid",
		"control_raft_member_invalid",
		"control_raft_invalid_member",
		"control_raft_invalid_addr",
	):
		return httpx.NewError(http.StatusBadRequest, message, err)
	case hasControlOopsCode(err, "node_not_found"):
		return httpx.NewError(http.StatusNotFound, message, err)
	default:
		return httpx.NewError(http.StatusInternalServerError, message, err)
	}
}

func (s *ServiceRuntime) nowUnix() int64 {
	return s.nowTime().Unix()
}

func (s *ServiceRuntime) nowTime() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func hasControlOopsCode(err error, codes ...string) bool {
	code, ok := controlOopsCode(err)
	return ok && slices.Contains(codes, code)
}

func controlOopsCode(err error) (string, bool) {
	for current := err; current != nil; current = errors.Unwrap(current) {
		oopsErr, ok := oops.AsOops(current)
		if !ok {
			continue
		}
		code, ok := oopsErr.Code().(string)
		if !ok {
			continue
		}
		return code, true
	}
	return "", false
}

func RestoreSnapshot(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	path := strings.TrimSpace(svc.cfg.Persistence.SnapshotPath)
	if path == "" {
		return nil
	}
	raftDir := strings.TrimSpace(svc.cfg.Raft.NodeHostDir)
	if raftDir != "" {
		hasData, err := hasExistingDragonboatData(raftDir)
		if err != nil {
			return err
		}
		if hasData {
			logger.Info("control snapshot restore skipped; dragonboat data exists", "path", path, "raft_dir", raftDir)
			return nil
		}
	}
	snapshot, err := LoadSnapshotFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := svc.state.RestoreSnapshot(snapshot); err != nil {
		return err
	}
	logger.Info("control snapshot restored", "path", path, "revision", svc.state.Revision())
	return nil
}

func SaveSnapshot(_ context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	path := strings.TrimSpace(svc.cfg.Persistence.SnapshotPath)
	if path == "" {
		return nil
	}
	snapshot := svc.state.ExportSnapshot()
	if err := SaveSnapshotFile(path, snapshot); err != nil {
		return err
	}
	logger.Info("control snapshot saved", "path", path, "revision", snapshot.Revision)
	return nil
}

func SubscribeRebalanceEvents(_ context.Context, logger *slog.Logger, bus eventx.BusRuntime) error {
	_, err := eventx.Subscribe[RebalanceEvent](bus, func(_ context.Context, event RebalanceEvent) error {
		body := event.Event
		logger.Info("control rebalance event",
			"event_id", body.ID,
			"revision", body.Revision,
			"reason", body.Reason,
			"node_id", body.NodeID,
			"state", body.State,
			"namespace", body.Namespace,
			"space", body.Space,
			"route_count", body.RouteCount,
		)
		return nil
	})
	return err
}

func (s *ServiceRuntime) apply(ctx context.Context, command Command) (ApplyResult, error) {
	if raft := s.dragonboatRuntime(); raft != nil {
		return raft.Propose(ctx, command)
	}
	return s.fsm.Apply(ctx, command)
}

func (s *ServiceRuntime) dragonboatRuntime() *DragonboatRuntime {
	s.raftMu.RLock()
	defer s.raftMu.RUnlock()
	return s.raft
}

func (s *ServiceRuntime) setDragonboatRuntime(raft *DragonboatRuntime) {
	s.raftMu.Lock()
	defer s.raftMu.Unlock()
	s.raft = raft
}

func (s *ServiceRuntime) clearDragonboatRuntime(raft *DragonboatRuntime) {
	s.raftMu.Lock()
	defer s.raftMu.Unlock()
	if s.raft == raft {
		s.raft = nil
	}
}
