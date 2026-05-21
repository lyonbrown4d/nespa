package node

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
)

type Config struct {
	Addr                        string
	ControlAddr                 string
	NodeID                      string
	HeartbeatInterval           time.Duration
	SnapshotPath                string
	SnapshotInterval            time.Duration
	ReplicationOutboxPath       string
	DefaultNamespaceMemoryBytes uint64
	DefaultSpaceMemoryBytes     uint64
}

type ServiceRuntime struct {
	cfg               Config
	controlClient     *ControlClient
	heartbeatInterval time.Duration
	routeEpoch        atomic.Uint64
	snapshotMu        sync.RWMutex
	snapshot          controlapi.SnapshotBody
}

func NewServiceRuntime(cfg Config, _ cache.Service) *ServiceRuntime {
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	return &ServiceRuntime{
		cfg:               cfg,
		controlClient:     NewControlClient(cfg.ControlAddr),
		heartbeatInterval: heartbeatInterval,
	}
}

func StartControlRegistration(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	if strings.TrimSpace(svc.cfg.ControlAddr) == "" {
		return nil
	}
	registerWithControl(ctx, logger, svc)
	go runControlHeartbeat(ctx, logger, svc)
	return nil
}

func (s *ServiceRuntime) RouteEpoch() uint64 {
	return s.routeEpoch.Load()
}

func (s *ServiceRuntime) ReplicationTargets(key cachewire.Key) []string {
	s.snapshotMu.RLock()
	snapshot := s.snapshot
	s.snapshotMu.RUnlock()

	route, ok := routing.Select(snapshot.Routes, key.Namespace, key.Space, key.Key)
	if !ok || !s.primaryForRoute(route) {
		return nil
	}
	return replicaAddrs(route.Replicas, s.cfg)
}

func (s *ServiceRuntime) primaryForRoute(route controlapi.RouteBody) bool {
	return (route.NodeID != "" && route.NodeID == s.cfg.NodeID) || (route.Addr != "" && route.Addr == s.cfg.Addr)
}

func replicaAddrs(replicas []controlapi.RouteReplicaBody, cfg Config) []string {
	addrs := make([]string, 0, len(replicas))
	for index := range replicas {
		replica := replicas[index]
		if replica.Addr == "" || replica.Addr == cfg.Addr || replica.NodeID == cfg.NodeID {
			continue
		}
		addrs = append(addrs, replica.Addr)
	}
	return addrs
}

func (s *ServiceRuntime) observeRevision(revision uint64) {
	for {
		current := s.routeEpoch.Load()
		if revision <= current || s.routeEpoch.CompareAndSwap(current, revision) {
			return
		}
	}
}

func (s *ServiceRuntime) observeSnapshot(snapshot controlapi.SnapshotBody) {
	s.snapshotMu.Lock()
	s.snapshot = snapshot
	s.snapshotMu.Unlock()
	s.observeRevision(snapshot.Revision)
}

func registerWithControl(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) {
	resp, err := svc.controlClient.RegisterNode(ctx, controlapi.RegisterNodeBody{
		NodeID: svc.cfg.NodeID,
		Addr:   svc.cfg.Addr,
	})
	if err != nil {
		logger.Warn("node control-plane registration failed", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "error", err)
		return
	}
	refreshControlSnapshot(ctx, logger, svc, resp.Revision)
	logger.Info("node registered with control-plane", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "revision", resp.Revision)
}

func runControlHeartbeat(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) {
	ticker := time.NewTicker(svc.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := svc.controlClient.Heartbeat(ctx, controlapi.HeartbeatBody{
				NodeID: svc.cfg.NodeID,
				Addr:   svc.cfg.Addr,
			})
			if err != nil {
				logger.Warn("node control-plane heartbeat failed", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "error", err)
				continue
			}
			refreshControlSnapshot(ctx, logger, svc, resp.Revision)
			logger.Debug("node control-plane heartbeat sent", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "revision", resp.Revision)
		}
	}
}

func refreshControlSnapshot(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime, revision uint64) {
	if revision <= svc.RouteEpoch() {
		return
	}
	snapshot, err := svc.controlClient.Snapshot(ctx)
	if err != nil {
		svc.observeRevision(revision)
		logger.Warn("node control-plane snapshot refresh failed", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "revision", revision, "error", err)
		return
	}
	svc.observeSnapshot(snapshot)
}
