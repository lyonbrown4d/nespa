package node

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type Config struct {
	Addr                        string
	ControlAddr                 string
	NodeID                      string
	HeartbeatInterval           time.Duration
	SnapshotPath                string
	DefaultNamespaceMemoryBytes uint64
	DefaultSpaceMemoryBytes     uint64
}

type ServiceRuntime struct {
	cfg               Config
	controlClient     *ControlClient
	heartbeatInterval time.Duration
	routeEpoch        atomic.Uint64
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

func (s *ServiceRuntime) observeRevision(revision uint64) {
	for {
		current := s.routeEpoch.Load()
		if revision <= current || s.routeEpoch.CompareAndSwap(current, revision) {
			return
		}
	}
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
	svc.observeRevision(resp.Revision)
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
			svc.observeRevision(resp.Revision)
			logger.Debug("node control-plane heartbeat sent", "node_id", svc.cfg.NodeID, "control_addr", svc.cfg.ControlAddr, "revision", resp.Revision)
		}
	}
}
