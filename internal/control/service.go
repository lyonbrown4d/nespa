package control

import (
	"context"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/runtime"
)

type Config struct {
	Addr      string
	ClusterID string
}

type StateBody struct {
	ClusterID string `json:"cluster_id"`
	Revision  uint64 `json:"revision"`
	Mode      string `json:"mode"`
}

func Module(cfg Config) dix.Module {
	return runtime.HTTPModule(runtime.HTTPConfig{
		Name: "control",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"cluster_id": cfg.ClusterID,
			"role":       "control-plane",
		},
		Routes: func(server httpx.ServerRuntime) {
			httpx.MustGet(server, "/v1/control/state", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[StateBody], error) {
				return runtime.JSON(StateBody{
					ClusterID: cfg.ClusterID,
					Revision:  0,
					Mode:      "bootstrap",
				}), nil
			})
		},
	})
}
