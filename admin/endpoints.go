package admin

import (
	"context"
	"fmt"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
)

type Endpoint interface {
	httpx.Endpoint
	adminEndpoint()
}

type summaryCacheProvider interface {
	Stats(context.Context) (cache.Stats, error)
}

type summaryControlProvider interface {
	Namespaces() controlapi.NamespacesBody
	Spaces() controlapi.SpacesBody
	Nodes() controlapi.NodesBody
}

type summaryEndpoint struct {
	cfg        Config
	cacheSvc   summaryCacheProvider
	controlSvc summaryControlProvider
}

func NewSummaryEndpoint(cfg Config, cacheSvc summaryCacheProvider, controlSvc summaryControlProvider) Endpoint {
	return &summaryEndpoint{
		cfg:        cfg,
		cacheSvc:   cacheSvc,
		controlSvc: controlSvc,
	}
}

func (e *summaryEndpoint) adminEndpoint() {}

func (e *summaryEndpoint) EndpointSpec() httpx.EndpointSpec {
	return httpx.EndpointSpec{
		Prefix: "/v1/admin",
	}
}

func (e *summaryEndpoint) Register(registrar httpx.Registrar) {
	httpx.MustGroupGet(registrar.Scope(), "/summary", e.Summary)
}

func (e *summaryEndpoint) Summary(ctx context.Context, _ *runtime.EmptyInput) (*runtime.JSONResponse[SummaryBody], error) {
	stats, err := e.cacheSvc.Stats(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cache stats: %w", err)
	}

	namespaces := e.controlSvc.Namespaces()
	spaces := e.controlSvc.Spaces()
	nodes := e.controlSvc.Nodes()

	return runtime.JSON(SummaryBody{
		ControlAddr:        e.cfg.ControlAddr,
		Namespaces:         uint64(len(namespaces.Namespaces)),
		Spaces:             uint64(len(spaces.Spaces)),
		Nodes:              uint64(len(nodes.Nodes)),
		CacheMemory:        stats.MemoryBytes,
		CacheObjects:       stats.Objects,
		CacheGetRequests:   stats.GetRequests,
		CacheGetHits:       stats.GetHits,
		CacheGetMisses:     stats.GetMisses,
		CacheGetExpired:    stats.GetExpired,
		CacheTouchRequests: stats.TouchRequests,
		CacheTouchHits:     stats.TouchHits,
		CacheTouchMisses:   stats.TouchMisses,
		CacheEvictions:     stats.Evictions,
	}), nil
}
