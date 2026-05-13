// Package admin exposes the Nespa administrative HTTP API.
package admin

import (
	"context"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/runtime"
)

type Config struct {
	Addr        string
	ControlAddr string
}

type SummaryBody struct {
	ControlAddr string `json:"control_addr"`
	Namespaces  uint64 `json:"namespaces"`
	Spaces      uint64 `json:"spaces"`
	Nodes       uint64 `json:"nodes"`
}

func HTTPConfig(cfg Config) runtime.HTTPConfig {
	return runtime.HTTPConfig{
		Name: "admin",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"control_addr": cfg.ControlAddr,
			"role":         "admin-api",
		},
		Routes: func(server httpx.ServerRuntime) {
			httpx.MustGet(server, "/v1/admin/summary", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[SummaryBody], error) {
				return runtime.JSON(SummaryBody{
					ControlAddr: cfg.ControlAddr,
					Namespaces:  0,
					Spaces:      0,
					Nodes:       0,
				}), nil
			})
		},
	}
}
