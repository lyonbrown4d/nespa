package frontend

import (
	"context"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/runtime"
)

type Config struct {
	Addr        string
	ControlAddr string
}

type RoutesBody struct {
	RouteEpoch uint64 `json:"route_epoch"`
	Source     string `json:"source"`
	Routes     []any  `json:"routes"`
}

func Module(cfg Config) dix.Module {
	return runtime.HTTPModule(runtime.HTTPConfig{
		Name: "frontend",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"control_addr": cfg.ControlAddr,
			"role":         "gateway",
		},
		Routes: func(server httpx.ServerRuntime) {
			httpx.MustGet(server, "/v1/frontend/routes", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[RoutesBody], error) {
				return runtime.JSON(RoutesBody{
					RouteEpoch: 0,
					Source:     cfg.ControlAddr,
					Routes:     []any{},
				}), nil
			})
		},
	})
}
