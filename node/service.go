package node

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type Config struct {
	Addr                        string
	ControlAddr                 string
	NodeID                      string
	HeartbeatInterval           time.Duration
	DefaultNamespaceMemoryBytes uint64
	DefaultSpaceMemoryBytes     uint64
}

type ServiceRuntime struct {
	cfg               Config
	controlClient     *ControlClient
	heartbeatInterval time.Duration
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
	registerWithControl(ctx, logger, svc.controlClient, svc.cfg)
	go runControlHeartbeat(ctx, logger, svc.controlClient, svc.cfg, svc.heartbeatInterval)
	return nil
}

func registerWithControl(ctx context.Context, logger *slog.Logger, client *ControlClient, cfg Config) {
	resp, err := client.RegisterNode(ctx, controlapi.RegisterNodeBody{
		NodeID: cfg.NodeID,
		Addr:   cfg.Addr,
	})
	if err != nil {
		logger.Warn("node control-plane registration failed", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "error", err)
		return
	}
	logger.Info("node registered with control-plane", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "revision", resp.Revision)
}

func runControlHeartbeat(ctx context.Context, logger *slog.Logger, client *ControlClient, cfg Config, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.Heartbeat(ctx, controlapi.HeartbeatBody{
				NodeID: cfg.NodeID,
				Addr:   cfg.Addr,
			})
			if err != nil {
				logger.Warn("node control-plane heartbeat failed", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "error", err)
				continue
			}
			logger.Debug("node control-plane heartbeat sent", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "revision", resp.Revision)
		}
	}
}
