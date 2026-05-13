package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cacheapi"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
)

type Config struct {
	Addr                        string
	ControlAddr                 string
	NodeID                      string
	HeartbeatInterval           time.Duration
	DefaultNamespaceMemoryBytes uint64
	DefaultSpaceMemoryBytes     uint64
}

type StatsBody struct {
	NodeID      string             `json:"node_id"`
	Objects     uint64             `json:"objects"`
	MemoryBytes uint64             `json:"memory_bytes"`
	Evictions   uint64             `json:"evictions"`
	Shards      []cache.ShardStats `json:"shards"`
	Spaces      []cache.SpaceStats `json:"spaces"`
}

type ServiceRuntime struct {
	cfg               Config
	cacheSvc          cache.Service
	controlClient     *ControlClient
	heartbeatInterval time.Duration
}

func NewServiceRuntime(cfg Config, cacheSvc cache.Service) *ServiceRuntime {
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	return &ServiceRuntime{
		cfg:               cfg,
		cacheSvc:          cacheSvc,
		controlClient:     NewControlClient(cfg.ControlAddr),
		heartbeatInterval: heartbeatInterval,
	}
}

func HTTPConfig(svc *ServiceRuntime) runtime.HTTPConfig {
	cfg := svc.cfg
	return runtime.HTTPConfig{
		Name: "node",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"node_id": cfg.NodeID,
			"role":    "data-node",
		},
		Routes: func(server httpx.ServerRuntime) {
			registerNodeRoutes(server, svc)
		},
	}
}

func StartControlRegistration(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	if strings.TrimSpace(svc.cfg.ControlAddr) == "" {
		return nil
	}
	registerWithControl(ctx, logger, svc.controlClient, svc.cfg)
	go runControlHeartbeat(ctx, logger, svc.controlClient, svc.cfg, svc.heartbeatInterval)
	return nil
}

func registerNodeRoutes(server httpx.ServerRuntime, svc *ServiceRuntime) {
	httpx.MustGet(server, "/v1/node/stats", nodeStatsHandler(svc))
	httpx.MustPut(server, "/v1/node/cache", nodeSetHandler(svc))
	httpx.MustGet(server, "/v1/node/cache", nodeGetHandler(svc))
	httpx.MustDelete(server, "/v1/node/cache", nodeDeleteHandler(svc))
}

func nodeStatsHandler(svc *ServiceRuntime) func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[StatsBody], error) {
	return func(ctx context.Context, _ *runtime.EmptyInput) (*runtime.JSONResponse[StatsBody], error) {
		stats, err := svc.cacheSvc.Stats(ctx)
		if err != nil {
			return nil, fmt.Errorf("read node stats: %w", err)
		}
		return runtime.JSON(StatsBody{
			NodeID:      svc.cfg.NodeID,
			Objects:     stats.Objects,
			MemoryBytes: stats.MemoryBytes,
			Evictions:   stats.Evictions,
			Shards:      stats.Shards,
			Spaces:      stats.Spaces,
		}), nil
	}
}

func nodeSetHandler(svc *ServiceRuntime) func(context.Context, *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
	return func(ctx context.Context, input *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
		rec, err := svc.cacheSvc.Set(ctx, cacheKey(input.Body.Namespace, input.Body.Space, input.Body.Entity, input.Body.Key), []byte(input.Body.Value), cache.SetOptions{
			TTL:              ttlFromMillis(input.Body.TTLMillis),
			NamespaceVersion: input.Body.NamespaceVersion,
			SpaceVersion:     input.Body.SpaceVersion,
		})
		if err != nil {
			return nil, mapCacheError(err)
		}
		return runtime.JSON(cacheRecordBody(rec, true)), nil
	}
}

func nodeGetHandler(svc *ServiceRuntime) func(context.Context, *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
	return func(ctx context.Context, input *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
		rec, ok, err := svc.cacheSvc.Get(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key), cache.GetOptions{
			NamespaceVersion: input.NamespaceVersion,
			SpaceVersion:     input.SpaceVersion,
		})
		if err != nil {
			return nil, mapCacheError(err)
		}
		if !ok {
			return runtime.JSON(cacheapi.RecordBody{Found: false}), nil
		}
		return runtime.JSON(cacheRecordBody(rec, true)), nil
	}
}

func nodeDeleteHandler(svc *ServiceRuntime) func(context.Context, *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
	return func(ctx context.Context, input *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
		deleted, err := svc.cacheSvc.Delete(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key))
		if err != nil {
			return nil, mapCacheError(err)
		}
		return runtime.JSON(cacheapi.DeleteBody{Deleted: deleted}), nil
	}
}

func registerWithControl(ctx context.Context, logger *slog.Logger, client *ControlClient, cfg Config) {
	resp, err := client.RegisterNode(ctx, controlapi.RegisterNodeBody{
		NodeID: cfg.NodeID,
		Addr:   cfg.Addr,
	})
	if err != nil {
		logger.Warn("node control-plane registration failed", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "error", err)
		return
	}
	logger.Info("node registered with control-plane", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "revision", resp.Revision)
}

func runControlHeartbeat(ctx context.Context, logger *slog.Logger, client *ControlClient, cfg Config, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.Heartbeat(ctx, controlapi.HeartbeatBody{
				NodeID: cfg.NodeID,
				Addr:   cfg.Addr,
			})
			if err != nil {
				logger.Warn("node control-plane heartbeat failed", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "error", err)
				continue
			}
			logger.Debug("node control-plane heartbeat sent", "node_id", cfg.NodeID, "control_addr", cfg.ControlAddr, "revision", resp.Revision)
		}
	}
}

func cacheKey(namespace, space, entity, key string) cache.Key {
	return cache.Key{
		Namespace: namespace,
		Space:     space,
		Entity:    entity,
		Key:       key,
	}
}

func ttlFromMillis(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func cacheRecordBody(rec cache.Record, found bool) cacheapi.RecordBody {
	out := cacheapi.RecordBody{
		Found:            found,
		Namespace:        rec.Key.Namespace,
		Space:            rec.Key.Space,
		Entity:           rec.Key.Entity,
		Key:              rec.Key.Key,
		Value:            string(rec.Value),
		Version:          rec.Version,
		NamespaceVersion: rec.NamespaceVersion,
		SpaceVersion:     rec.SpaceVersion,
	}
	if !rec.ExpireAt.IsZero() {
		out.ExpireAtUnixMs = rec.ExpireAt.UnixMilli()
	}
	return out
}

func mapCacheError(err error) error {
	switch {
	case errors.Is(err, cache.ErrQuotaExceeded):
		return httpx.NewError(http.StatusTooManyRequests, "cache quota exceeded", err)
	case errors.Is(err, engine.ErrInvalidKey):
		return httpx.NewError(http.StatusBadRequest, "invalid cache key", err)
	default:
		return err
	}
}
