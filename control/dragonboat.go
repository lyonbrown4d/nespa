package control

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	dragonboat "github.com/lni/dragonboat/v3"
	dragonclient "github.com/lni/dragonboat/v3/client"
	dragonconfig "github.com/lni/dragonboat/v3/config"
	dragonstatemachine "github.com/lni/dragonboat/v3/statemachine"
	"github.com/samber/oops"
)

const (
	defaultControlRaftAddr                  = "127.0.0.1:7601"
	defaultControlRaftClusterID      uint64 = 1
	defaultControlRaftNodeID         uint64 = 1
	defaultControlRaftRTTMillis      uint64 = 100
	defaultControlProposalWait              = 5 * time.Second
	defaultControlSnapshotEntries           = 1024
	defaultControlCompactionOverhead        = 128
)

type RaftConfig struct {
	NodeHostDir        string
	Addr               string
	ClusterID          uint64
	NodeID             uint64
	ProposalTimeout    time.Duration
	SnapshotEntries    uint64
	CompactionOverhead uint64
}

type DragonboatRuntime struct {
	nodeHost        *dragonboat.NodeHost
	session         *dragonclient.Session
	clusterID       uint64
	nodeID          uint64
	addr            string
	nodeHostDir     string
	tempNodeHostDir string
	proposalTimeout time.Duration
}

func StartDragonboat(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	if svc.dragonboatRuntime() != nil {
		return nil
	}

	runtime, err := newDragonboatRuntime(svc.cfg.Raft, svc.state, svc.fsm)
	if err != nil {
		return err
	}
	if err := runtime.WaitReady(ctx); err != nil {
		return errors.Join(err, runtime.Stop())
	}
	svc.setDragonboatRuntime(runtime)

	if err := svc.proposeBootstrapNodes(ctx); err != nil {
		svc.clearDragonboatRuntime(runtime)
		return errors.Join(err, runtime.Stop())
	}

	logger.Info("control dragonboat started",
		"cluster_id", runtime.clusterID,
		"node_id", runtime.nodeID,
		"addr", runtime.addr,
		"dir", runtime.nodeHostDir,
	)
	return nil
}

func StopDragonboat(_ context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	runtime := svc.dragonboatRuntime()
	if runtime == nil {
		return nil
	}

	err := runtime.Stop()
	svc.clearDragonboatRuntime(runtime)
	if err != nil {
		return err
	}
	logger.Info("control dragonboat stopped", "cluster_id", runtime.clusterID, "node_id", runtime.nodeID)
	return nil
}

func (r *DragonboatRuntime) Propose(ctx context.Context, command Command) (ApplyResult, error) {
	raw, err := json.Marshal(command)
	if err != nil {
		return ApplyResult{}, oops.Code("control_raft_command_encode_failed").
			In("control.raft").
			Wrapf(err, "encode control raft command")
	}

	result, err := r.syncPropose(ctx, raw)
	if err != nil {
		return ApplyResult{}, oops.Code("control_raft_propose_failed").
			In("control.raft").
			With("command", command.Type).
			Wrapf(err, "propose control raft command")
	}

	envelope, err := decodeDragonboatApplyEnvelope(result.Data)
	if err != nil {
		return ApplyResult{}, err
	}
	if envelope.Error != nil {
		return envelope.Result, envelope.Error.toError()
	}
	return envelope.Result, nil
}

func (r *DragonboatRuntime) WaitReady(ctx context.Context) error {
	if _, err := r.syncRead(ctx); err != nil {
		return oops.Code("control_raft_readiness_failed").
			In("control.raft").
			Wrapf(err, "wait for control dragonboat readiness")
	}
	return nil
}

func (r *DragonboatRuntime) Stop() error {
	r.nodeHost.Stop()
	if r.tempNodeHostDir == "" {
		return nil
	}
	if err := os.RemoveAll(r.tempNodeHostDir); err != nil {
		return oops.Code("control_raft_temp_cleanup_failed").
			In("control.raft").
			With("dir", r.tempNodeHostDir).
			Wrapf(err, "remove temporary control raft directory")
	}
	return nil
}

func newDragonboatRuntime(cfg RaftConfig, state *ControlState, fsm *ControlFSM) (*DragonboatRuntime, error) {
	cfg = normalizeRaftConfig(cfg)
	nodeHostDir, tempNodeHostDir, err := resolveNodeHostDir(cfg.NodeHostDir)
	if err != nil {
		return nil, err
	}

	hasData, err := hasExistingDragonboatData(nodeHostDir)
	if err != nil {
		return nil, err
	}

	nodeHost, err := dragonboat.NewNodeHost(dragonconfig.NodeHostConfig{
		NodeHostDir:    nodeHostDir,
		RTTMillisecond: defaultControlRaftRTTMillis,
		RaftAddress:    cfg.Addr,
	})
	if err != nil {
		return nil, errors.Join(wrapControlRaftStartError(err), removeTempNodeHostDir(tempNodeHostDir))
	}

	initialMembers := map[uint64]dragonboat.Target(nil)
	if !hasData {
		initialMembers = map[uint64]dragonboat.Target{
			cfg.NodeID: cfg.Addr,
		}
	}

	createStateMachine := func(uint64, uint64) dragonstatemachine.IStateMachine {
		return &dragonboatStateMachine{state: state, fsm: fsm}
	}
	raftConfig := dragonconfig.Config{
		NodeID:             cfg.NodeID,
		ClusterID:          cfg.ClusterID,
		HeartbeatRTT:       1,
		ElectionRTT:        10,
		CheckQuorum:        true,
		SnapshotEntries:    cfg.SnapshotEntries,
		CompactionOverhead: cfg.CompactionOverhead,
	}
	if err := nodeHost.StartCluster(initialMembers, false, createStateMachine, raftConfig); err != nil {
		nodeHost.Stop()
		return nil, errors.Join(wrapControlRaftStartError(err), removeTempNodeHostDir(tempNodeHostDir))
	}

	return &DragonboatRuntime{
		nodeHost:        nodeHost,
		session:         nodeHost.GetNoOPSession(cfg.ClusterID),
		clusterID:       cfg.ClusterID,
		nodeID:          cfg.NodeID,
		addr:            cfg.Addr,
		nodeHostDir:     nodeHostDir,
		tempNodeHostDir: tempNodeHostDir,
		proposalTimeout: cfg.ProposalTimeout,
	}, nil
}

func (s *ServiceRuntime) proposeBootstrapNodes(ctx context.Context) error {
	for _, node := range s.cfg.BootstrapNodes {
		if _, err := s.RegisterNode(ctx, node.NodeID, node.Addr); err != nil {
			return oops.Code("control_raft_bootstrap_failed").
				In("control.raft").
				With("node_id", node.NodeID).
				Wrapf(err, "propose bootstrap control node")
		}
	}
	return nil
}

func normalizeRaftConfig(cfg RaftConfig) RaftConfig {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	if cfg.Addr == "" {
		cfg.Addr = defaultControlRaftAddr
	}
	if cfg.ClusterID == 0 {
		cfg.ClusterID = defaultControlRaftClusterID
	}
	if cfg.NodeID == 0 {
		cfg.NodeID = defaultControlRaftNodeID
	}
	if cfg.ProposalTimeout <= 0 {
		cfg.ProposalTimeout = defaultControlProposalWait
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = defaultControlSnapshotEntries
	}
	if cfg.CompactionOverhead == 0 {
		cfg.CompactionOverhead = defaultControlCompactionOverhead
	}
	return cfg
}

func resolveNodeHostDir(path string) (string, string, error) {
	path = strings.TrimSpace(path)
	if path != "" {
		return filepath.Clean(path), "", nil
	}

	dir, err := os.MkdirTemp("", "nespa-control-raft-*")
	if err != nil {
		return "", "", oops.Code("control_raft_temp_dir_failed").
			In("control.raft").
			Wrapf(err, "create temporary control raft directory")
	}
	return dir, dir, nil
}

func hasExistingDragonboatData(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, oops.Code("control_raft_dir_read_failed").
			In("control.raft").
			With("dir", dir).
			Wrapf(err, "read control raft directory")
	}
	return len(entries) > 0, nil
}

func removeTempNodeHostDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return oops.Code("control_raft_temp_cleanup_failed").
			In("control.raft").
			With("dir", dir).
			Wrapf(err, "remove temporary control raft directory")
	}
	return nil
}

func wrapControlRaftStartError(err error) error {
	return oops.Code("control_raft_start_failed").
		In("control.raft").
		Wrapf(err, "start control dragonboat")
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}
