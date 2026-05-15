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
	ControlAddr        string `json:"control_addr"`
	Namespaces         uint64 `json:"namespaces"`
	Spaces             uint64 `json:"spaces"`
	Nodes              uint64 `json:"nodes"`
	ControlRevision    uint64 `json:"control_revision"`
	RouteCount         uint64 `json:"route_count"`
	NodeRouteEpoch     uint64 `json:"node_route_epoch"`
	CacheMemory        uint64 `json:"cache_memory_bytes"`
	CacheObjects       uint64 `json:"cache_objects"`
	CacheGetRequests   uint64 `json:"cache_get_requests"`
	CacheGetHits       uint64 `json:"cache_get_hits"`
	CacheGetMisses     uint64 `json:"cache_get_misses"`
	CacheGetExpired    uint64 `json:"cache_get_expired"`
	CacheTouchRequests uint64 `json:"cache_touch_requests"`
	CacheTouchHits     uint64 `json:"cache_touch_hits"`
	CacheTouchMisses   uint64 `json:"cache_touch_misses"`
	CacheEvictions     uint64 `json:"cache_evictions"`
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
