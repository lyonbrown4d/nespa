package control

import (
	"context"
	"log/slog"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/controlapi"
	"github.com/lyonbrown4d/nespa/internal/runtime"
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

func Module(cfg Config) dix.Module {
	state := NewControlState(cfg.ClusterID)
	for _, node := range cfg.BootstrapNodes {
		if node.NodeID != "" && node.Addr != "" {
			state.RegisterNode(node.NodeID, node.Addr)
		}
	}

	liveness := normalizeLivenessConfig(cfg.Liveness)

	httpModule := runtime.HTTPModule(runtime.HTTPConfig{
		Name: "control",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"cluster_id": cfg.ClusterID,
			"role":       "control-plane",
		},
		Routes: func(server httpx.ServerRuntime) {
			httpx.MustGet(server, "/v1/control/state", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.StateBody], error) {
				return runtime.JSON(state.State()), nil
			})

			httpx.MustGet(server, "/v1/control/snapshot", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SnapshotBody], error) {
				return runtime.JSON(state.Snapshot()), nil
			})

			httpx.MustGet(server, "/v1/control/nodes", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NodesBody], error) {
				return runtime.JSON(state.Nodes()), nil
			})

			httpx.MustPost(server, "/v1/control/nodes", func(_ context.Context, input *controlapi.RegisterNodeInput) (*runtime.JSONResponse[controlapi.RegisterNodeResponse], error) {
				return runtime.JSON(state.RegisterNode(input.Body.NodeID, input.Body.Addr)), nil
			})

			httpx.MustPut(server, "/v1/control/nodes/heartbeat", func(_ context.Context, input *controlapi.HeartbeatInput) (*runtime.JSONResponse[controlapi.HeartbeatResponse], error) {
				return runtime.JSON(state.Heartbeat(input.Body.NodeID, input.Body.Addr)), nil
			})
		},
	})

	return dix.NewModule("control",
		dix.WithModuleImports(httpModule),
		dix.WithModuleHooks(
			dix.OnStart[*slog.Logger](func(ctx context.Context, logger *slog.Logger) error {
				go runLivenessSweep(ctx, logger, state, liveness)
				return nil
			}, dix.LifecycleName("control.liveness.start"), dix.LifecycleAfter("control.http.start")),
		),
	)
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
