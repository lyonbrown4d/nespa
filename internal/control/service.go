package control

import (
	"context"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/controlapi"
	"github.com/lyonbrown4d/nespa/internal/runtime"
)

type Config struct {
	Addr           string
	ClusterID      string
	BootstrapNodes []controlapi.RegisterNodeBody
}

func Module(cfg Config) dix.Module {
	state := NewControlState(cfg.ClusterID)
	for _, node := range cfg.BootstrapNodes {
		if node.NodeID != "" && node.Addr != "" {
			state.RegisterNode(node.NodeID, node.Addr)
		}
	}

	return runtime.HTTPModule(runtime.HTTPConfig{
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

			httpx.MustPost(server, "/v1/control/nodes", func(_ context.Context, input *controlapi.RegisterNodeInput) (*runtime.JSONResponse[controlapi.RegisterNodeResponse], error) {
				return runtime.JSON(state.RegisterNode(input.Body.NodeID, input.Body.Addr)), nil
			})

			httpx.MustPut(server, "/v1/control/nodes/heartbeat", func(_ context.Context, input *controlapi.HeartbeatInput) (*runtime.JSONResponse[controlapi.HeartbeatResponse], error) {
				return runtime.JSON(state.Heartbeat(input.Body.NodeID, input.Body.Addr)), nil
			})
		},
	})
}
