package node

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/node/cache"
	"github.com/lyonbrown4d/nespa/internal/node/engine"
	"github.com/lyonbrown4d/nespa/internal/runtime"
)

type Config struct {
	Addr                        string
	NodeID                      string
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

type CacheSetInput struct {
	Body CacheSetBody
}

type CacheSetBody struct {
	Namespace        string `json:"namespace"`
	Space            string `json:"space"`
	Entity           string `json:"entity,omitempty"`
	Key              string `json:"key"`
	Value            string `json:"value"`
	TTLMillis        int64  `json:"ttl_ms,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
}

type CacheGetInput struct {
	Namespace        string `query:"namespace"`
	Space            string `query:"space"`
	Entity           string `query:"entity"`
	Key              string `query:"key"`
	NamespaceVersion uint64 `query:"namespace_version"`
	SpaceVersion     uint64 `query:"space_version"`
}

type CacheDeleteInput struct {
	Namespace string `query:"namespace"`
	Space     string `query:"space"`
	Entity    string `query:"entity"`
	Key       string `query:"key"`
}

type CacheRecordBody struct {
	Found            bool   `json:"found"`
	Namespace        string `json:"namespace,omitempty"`
	Space            string `json:"space,omitempty"`
	Entity           string `json:"entity,omitempty"`
	Key              string `json:"key,omitempty"`
	Value            string `json:"value,omitempty"`
	Version          uint64 `json:"version,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpireAtUnixMs   int64  `json:"expire_at_ms,omitempty"`
}

type CacheDeleteBody struct {
	Deleted bool `json:"deleted"`
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

					httpx.MustPut(server, "/v1/node/cache", func(ctx context.Context, input *CacheSetInput) (*runtime.JSONResponse[CacheRecordBody], error) {
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

					httpx.MustGet(server, "/v1/node/cache", func(ctx context.Context, input *CacheGetInput) (*runtime.JSONResponse[CacheRecordBody], error) {
						rec, ok, err := cacheSvc.Get(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key), cache.GetOptions{
							NamespaceVersion: input.NamespaceVersion,
							SpaceVersion:     input.SpaceVersion,
						})
						if err != nil {
							return nil, mapCacheError(err)
						}
						if !ok {
							return runtime.JSON(CacheRecordBody{Found: false}), nil
						}
						return runtime.JSON(cacheRecordBody(rec, true)), nil
					})

					httpx.MustDelete(server, "/v1/node/cache", func(ctx context.Context, input *CacheDeleteInput) (*runtime.JSONResponse[CacheDeleteBody], error) {
						deleted, err := cacheSvc.Delete(ctx, cacheKey(input.Namespace, input.Space, input.Entity, input.Key))
						if err != nil {
							return nil, mapCacheError(err)
						}
						return runtime.JSON(CacheDeleteBody{Deleted: deleted}), nil
					})
				},
			}),
		),
	)
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

func cacheRecordBody(rec cache.Record, found bool) CacheRecordBody {
	out := CacheRecordBody{
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
