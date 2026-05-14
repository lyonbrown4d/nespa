// Package admin exposes the Nespa administrative HTTP API.
package admin

import (
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
	}
}
