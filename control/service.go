// Package control implements the bootstrap control-plane service.
package control

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
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
}

type LivenessConfig struct {
	SweepInterval time.Duration
	SuspectAfter  time.Duration
	DeadAfter     time.Duration
}

type ServiceRuntime struct {
	cfg      Config
	state    *ControlState
	liveness LivenessConfig
}

func NewServiceRuntime(cfg Config) *ServiceRuntime {
	return NewServiceRuntimeWithEvents(cfg, nil)
}

func NewServiceRuntimeWithEvents(cfg Config, bus eventx.BusRuntime) *ServiceRuntime {
	state := NewControlStateWithEvents(cfg.ClusterID, bus)
	for _, node := range cfg.BootstrapNodes {
		if _, err := state.RegisterNode(context.Background(), node.NodeID, node.Addr); err != nil {
			continue
		}
	}

	return &ServiceRuntime{
		cfg:      cfg,
		state:    state,
		liveness: normalizeLivenessConfig(cfg.Liveness),
	}
}

func (s *ServiceRuntime) Namespaces() controlapi.NamespacesBody {
	return s.state.Namespaces()
}

func (s *ServiceRuntime) Spaces() controlapi.SpacesBody {
	return s.state.Spaces()
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
	case hasControlOopsCode(err, "invalid_node", "invalid_namespace", "invalid_space", "invalid_entity"):
		return httpx.NewError(http.StatusBadRequest, message, err)
	default:
		return httpx.NewError(http.StatusInternalServerError, message, err)
	}
}

func hasControlOopsCode(err error, codes ...string) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		oopsErr, ok := oops.AsOops(current)
		if !ok {
			continue
		}
		code, ok := oopsErr.Code().(string)
		if !ok {
			continue
		}
		if slices.Contains(codes, code) {
			return true
		}
	}
	return false
}

func StartLiveness(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	go runLivenessSweep(ctx, logger, svc.state, svc.liveness)
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

func normalizeLivenessConfig(cfg LivenessConfig) LivenessConfig {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Second
	}
	if cfg.SuspectAfter <= 0 {
		cfg.SuspectAfter = 15 * time.Second
	}
	if cfg.DeadAfter <= 0 {
		cfg.DeadAfter = 30 * time.Second
	}
	if cfg.DeadAfter < cfg.SuspectAfter {
		cfg.DeadAfter = cfg.SuspectAfter
	}
	return cfg
}

func runLivenessSweep(ctx context.Context, logger *slog.Logger, state *ControlState, cfg LivenessConfig) {
	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			result := state.AdvanceLiveness(ctx, now, cfg.SuspectAfter, cfg.DeadAfter)
			for _, node := range result.Changed {
				logger.Warn("control node liveness changed", "node_id", node.NodeID, "state", node.State, "revision", result.Revision)
			}
		}
	}
}
