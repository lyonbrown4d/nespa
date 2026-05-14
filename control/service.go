// Package control implements the bootstrap control-plane service.
package control

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
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
	state := NewControlState(cfg.ClusterID)
	for _, node := range cfg.BootstrapNodes {
		if _, err := state.RegisterNode(node.NodeID, node.Addr); err != nil {
			continue
		}
	}

	return &ServiceRuntime{
		cfg:      cfg,
		state:    state,
		liveness: normalizeLivenessConfig(cfg.Liveness),
	}
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
	case errors.Is(err, ErrNamespaceNotFound), errors.Is(err, ErrSpaceNotFound):
		return httpx.NewError(http.StatusNotFound, message, err)
	case errors.Is(err, ErrInvalidNode), errors.Is(err, ErrInvalidNamespace), errors.Is(err, ErrInvalidSpace), errors.Is(err, ErrInvalidEntity):
		return httpx.NewError(http.StatusBadRequest, message, err)
	default:
		return httpx.NewError(http.StatusInternalServerError, message, err)
	}
}

func StartLiveness(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	go runLivenessSweep(ctx, logger, svc.state, svc.liveness)
	return nil
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
			result := state.AdvanceLiveness(now, cfg.SuspectAfter, cfg.DeadAfter)
			for _, node := range result.Changed {
				logger.Warn("control node liveness changed", "node_id", node.NodeID, "state", node.State, "revision", result.Revision)
			}
		}
	}
}
