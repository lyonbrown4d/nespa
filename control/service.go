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
	state := svc.state
	return runtime.HTTPConfig{
		Name: "control",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"cluster_id": cfg.ClusterID,
			"role":       "control-plane",
		},
		Routes: func(server httpx.ServerRuntime) {
			registerReadRoutes(server, state)
			registerCatalogRoutes(server, state)
			registerNodeRoutes(server, state)
		},
	}
}

func registerReadRoutes(server httpx.ServerRuntime, state *ControlState) {
	httpx.MustGet(server, "/v1/control/state", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.StateBody], error) {
		return runtime.JSON(state.State()), nil
	})

	httpx.MustGet(server, "/v1/control/snapshot", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SnapshotBody], error) {
		return runtime.JSON(state.Snapshot()), nil
	})

	httpx.MustGet(server, "/v1/control/nodes", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NodesBody], error) {
		return runtime.JSON(state.Nodes()), nil
	})
}

func registerCatalogRoutes(server httpx.ServerRuntime, state *ControlState) {
	httpx.MustGet(server, "/v1/control/namespaces", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NamespacesBody], error) {
		return runtime.JSON(state.Namespaces()), nil
	})

	httpx.MustPost(server, "/v1/control/namespaces", func(_ context.Context, input *controlapi.CreateNamespaceInput) (*runtime.JSONResponse[controlapi.CreateNamespaceResponse], error) {
		response, err := state.CreateNamespace(input.Body.Namespace)
		if err != nil {
			return nil, controlStateError("create namespace failed", err)
		}
		return runtime.JSON(response), nil
	})

	httpx.MustPost(server, "/v1/control/namespaces/version-bump", func(_ context.Context, input *controlapi.BumpNamespaceVersionInput) (*runtime.JSONResponse[controlapi.BumpNamespaceVersionResponse], error) {
		response, err := state.BumpNamespaceVersion(input.Body.Namespace)
		if err != nil {
			return nil, controlStateError("bump namespace version failed", err)
		}
		return runtime.JSON(response), nil
	})

	httpx.MustGet(server, "/v1/control/spaces", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SpacesBody], error) {
		return runtime.JSON(state.Spaces()), nil
	})

	httpx.MustPost(server, "/v1/control/spaces", func(_ context.Context, input *controlapi.CreateSpaceInput) (*runtime.JSONResponse[controlapi.CreateSpaceResponse], error) {
		response, err := state.CreateSpace(input.Body.Namespace, input.Body.Space)
		if err != nil {
			return nil, controlStateError("create space failed", err)
		}
		return runtime.JSON(response), nil
	})

	httpx.MustPost(server, "/v1/control/spaces/version-bump", func(_ context.Context, input *controlapi.BumpSpaceVersionInput) (*runtime.JSONResponse[controlapi.BumpSpaceVersionResponse], error) {
		response, err := state.BumpSpaceVersion(input.Body.Namespace, input.Body.Space)
		if err != nil {
			return nil, controlStateError("bump space version failed", err)
		}
		return runtime.JSON(response), nil
	})
}

func registerNodeRoutes(server httpx.ServerRuntime, state *ControlState) {
	httpx.MustPost(server, "/v1/control/nodes", func(_ context.Context, input *controlapi.RegisterNodeInput) (*runtime.JSONResponse[controlapi.RegisterNodeResponse], error) {
		response, err := state.RegisterNode(input.Body.NodeID, input.Body.Addr)
		if err != nil {
			return nil, controlStateError("invalid node registration", err)
		}
		return runtime.JSON(response), nil
	})

	httpx.MustPut(server, "/v1/control/nodes/heartbeat", func(_ context.Context, input *controlapi.HeartbeatInput) (*runtime.JSONResponse[controlapi.HeartbeatResponse], error) {
		response, err := state.Heartbeat(input.Body.NodeID, input.Body.Addr)
		if err != nil {
			return nil, controlStateError("invalid node heartbeat", err)
		}
		return runtime.JSON(response), nil
	})
}

func controlStateError(message string, err error) error {
	switch {
	case errors.Is(err, ErrNamespaceNotFound), errors.Is(err, ErrSpaceNotFound):
		return httpx.NewError(http.StatusNotFound, message, err)
	case errors.Is(err, ErrInvalidNode), errors.Is(err, ErrInvalidNamespace), errors.Is(err, ErrInvalidSpace):
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
