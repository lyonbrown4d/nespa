package node

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/cacheapi"
	"github.com/lyonbrown4d/nespa/internal/controlapi"
	"github.com/lyonbrown4d/nespa/internal/node/cache"
	"github.com/lyonbrown4d/nespa/internal/node/engine"
	"github.com/lyonbrown4d/nespa/internal/runtime"
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

func Module(cfg Config) dix.Module {
	eng := engine.NewMemory(engine.Config{
		ShardCount:    16,
		SweepInterval: time.Second,
	})
	cacheSvc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{
		DefaultNamespaceMemoryBytes: cfg.DefaultNamespaceMemoryBytes,
		DefaultSpaceMemoryBytes:     cfg.DefaultSpaceMemoryBytes,
	}))
	controlClient := NewControlClient(cfg.ControlAddr)
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}

	return dix.NewModule("node",
		dix.WithModuleImports(
			engine.Module(eng, time.Second),
			cache.Module(cacheSvc),
			runtime.HTTPModule(runtime.HTTPConfig{
				Name: "node",
				Addr: cfg.Addr,
				Metadata: map[string]string{
					"node_id": cfg.NodeID,
					"role":    "data-node",
				},
				Routes: func(server httpx.ServerRuntime) {
					httpx.MustGet(server, "/v1/node/stats", func(ctx context.Context, _ *runtime.EmptyInput) (*runtime.JSONResponse[StatsBody], error) {
						stats, err := cacheSvc.Stats(ctx)
						if err != nil {
							return nil, err
						}
						return runtime.JSON(StatsBody{
							NodeID:      cfg.NodeID,
							Objects:     stats.Objects,
							MemoryBytes: stats.MemoryBytes,
							Evictions:   stats.Evictions,
							Shards:      stats.Shards,
							Spaces:      stats.Spaces,
						}), nil
					})

					httpx.MustPut(server, "/v1/node/cache", func(ctx context.Context, input *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
						rec, err := cacheSvc.Set(ctx, cacheKey(input.Body.Namespace, input.Body.Space, input.Body.Entity, input.Body.Key), []byte(input.Body.Value), cache.SetOptions{
							TTL:              ttlFromMillis(input.Body.TTLMillis),
							NamespaceVersion: input.Body.NamespaceVersion,
							SpaceVersion:     input.Body.SpaceVersion,
						})
						if err != nil {
							return nil, mapCacheError(err)
						}
						return runtime.JSON(cacheRecordBody(rec, true)), nil
					})

					httpx.MustGet(server, "/v1/node/cache", func(ctx context.Context, input *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
						rec, ok, err := cacheSvc.Get(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key), cache.GetOptions{
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
					})

					httpx.MustDelete(server, "/v1/node/cache", func(ctx context.Context, input *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
						deleted, err := cacheSvc.Delete(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key))
						if err != nil {
							return nil, mapCacheError(err)
						}
						return runtime.JSON(cacheapi.DeleteBody{Deleted: deleted}), nil
					})
				},
			}),
		),
		dix.WithModuleHooks(
			dix.OnStart[*slog.Logger](func(ctx context.Context, logger *slog.Logger) error {
				if strings.TrimSpace(cfg.ControlAddr) == "" {
					return nil
				}
				registerWithControl(ctx, logger, controlClient, cfg)
				go runControlHeartbeat(ctx, logger, controlClient, cfg, heartbeatInterval)
				return nil
			}, dix.LifecycleName("node.control.register"), dix.LifecycleAfter("node.http.start")),
		),
	)
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
