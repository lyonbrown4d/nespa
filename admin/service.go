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
	ControlAddr        string          `json:"control_addr"`
	Namespaces         uint64          `json:"namespaces"`
	Spaces             uint64          `json:"spaces"`
	Nodes              uint64          `json:"nodes"`
	ControlRevision    uint64          `json:"control_revision"`
	RouteCount         uint64          `json:"route_count"`
	NodeRouteEpoch     uint64          `json:"node_route_epoch"`
	CacheMemory        uint64          `json:"cache_memory_bytes"`
	CacheObjects       uint64          `json:"cache_objects"`
	CacheGetRequests   uint64          `json:"cache_get_requests"`
	CacheGetHits       uint64          `json:"cache_get_hits"`
	CacheGetMisses     uint64          `json:"cache_get_misses"`
	CacheGetExpired    uint64          `json:"cache_get_expired"`
	CacheTouchRequests uint64          `json:"cache_touch_requests"`
	CacheTouchHits     uint64          `json:"cache_touch_hits"`
	CacheTouchMisses   uint64          `json:"cache_touch_misses"`
	CacheEvictions     uint64          `json:"cache_evictions"`
	Replication        ReplicationBody `json:"replication"`
}

type ReplicationBody struct {
	QueueDepth          uint64 `json:"queue_depth"`
	QueueCapacity       uint64 `json:"queue_capacity"`
	Enqueued            uint64 `json:"enqueued"`
	Dropped             uint64 `json:"dropped"`
	Attempts            uint64 `json:"attempts"`
	Successes           uint64 `json:"successes"`
	Failures            uint64 `json:"failures"`
	OutboxEntries       uint64 `json:"outbox_entries"`
	OutboxFailures      uint64 `json:"outbox_failures"`
	AckTargets          uint64 `json:"ack_targets"`
	AckFailures         uint64 `json:"ack_failures"`
	LastQueuedSequence  uint64 `json:"last_queued_sequence"`
	LastAttemptSequence uint64 `json:"last_attempt_sequence"`
	LastSuccessSequence uint64 `json:"last_success_sequence"`
	LastFailureSequence uint64 `json:"last_failure_sequence"`
	LastDroppedSequence uint64 `json:"last_dropped_sequence"`
	LastOutboxSequence  uint64 `json:"last_outbox_sequence"`
	LastAckSequence     uint64 `json:"last_ack_sequence"`
	Retrying            bool   `json:"retrying"`
	ActiveTarget        string `json:"active_target,omitempty"`
	LastAckTarget       string `json:"last_ack_target,omitempty"`
	LastAckError        string `json:"last_ack_error,omitempty"`
	LastOutboxError     string `json:"last_outbox_error,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	LastErrorUnixMs     int64  `json:"last_error_unix_ms,omitempty"`
	LastSuccessUnixMs   int64  `json:"last_success_unix_ms,omitempty"`
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
