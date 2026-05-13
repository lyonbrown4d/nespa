package frontend

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/cacheapi"
	"github.com/lyonbrown4d/nespa/internal/runtime"
)

type Config struct {
	Addr        string
	ControlAddr string
	NodeAddr    string
}

func Module(cfg Config) dix.Module {
	initialRoutes := []Route{}
	if hasAddress(cfg.NodeAddr) {
		initialRoutes = append(initialRoutes, Route{Role: "data-node", Addr: cfg.NodeAddr, Weight: 1})
	}
	routeCache := NewRouteCache("bootstrap", initialRoutes)
	controlClient := NewControlClient(cfg.ControlAddr)
	nodeClient := NewNodeClient()

	httpModule := runtime.HTTPModule(runtime.HTTPConfig{
		Name: "frontend",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"control_addr": cfg.ControlAddr,
			"node_addr":    cfg.NodeAddr,
			"role":         "gateway",
		},
		Routes: func(server httpx.ServerRuntime) {
			httpx.MustGet(server, "/v1/frontend/routes", func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[RoutesBody], error) {
				return runtime.JSON(routeCache.Snapshot()), nil
			})

			httpx.MustPut(server, "/v1/cache", func(ctx context.Context, input *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
				route, ok := routeCache.Select(input.Body.Namespace, input.Body.Space)
				if !ok {
					return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
				}
				rec, err := nodeClient.Set(ctx, route.Addr, input.Body)
				if err != nil {
					return nil, err
				}
				return runtime.JSON(rec), nil
			})

			httpx.MustGet(server, "/v1/cache", func(ctx context.Context, input *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
				route, ok := routeCache.Select(input.Namespace, input.Space)
				if !ok {
					return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
				}
				rec, err := nodeClient.Get(ctx, route.Addr, *input)
				if err != nil {
					return nil, err
				}
				return runtime.JSON(rec), nil
			})

			httpx.MustDelete(server, "/v1/cache", func(ctx context.Context, input *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
				route, ok := routeCache.Select(input.Namespace, input.Space)
				if !ok {
					return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
				}
				out, err := nodeClient.Delete(ctx, route.Addr, *input)
				if err != nil {
					return nil, err
				}
				return runtime.JSON(out), nil
			})
		},
	})

	return dix.NewModule("frontend",
		dix.WithModuleImports(httpModule),
		dix.WithModuleHooks(
			dix.OnStart[*slog.Logger](func(ctx context.Context, logger *slog.Logger) error {
				if !hasAddress(cfg.ControlAddr) {
					return nil
				}
				refreshRoutes(ctx, logger, controlClient, routeCache, cfg.ControlAddr)
				go runRouteRefreshLoop(ctx, logger, controlClient, routeCache, cfg.ControlAddr, 2*time.Second)
				return nil
			}, dix.LifecycleName("frontend.routes.refresh"), dix.LifecycleBefore("frontend.http.start")),
		),
	)
}

func runRouteRefreshLoop(ctx context.Context, logger *slog.Logger, client *ControlClient, cache *RouteCache, source string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshRoutes(ctx, logger, client, cache, source)
		}
	}
}

func refreshRoutes(ctx context.Context, logger *slog.Logger, client *ControlClient, cache *RouteCache, source string) {
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		logger.Warn("frontend route snapshot refresh failed", "control_addr", source, "error", err)
		return
	}
	if cache.UpdateFromSnapshot(snapshot, source) {
		logger.Info("frontend route snapshot refreshed", "control_addr", source, "revision", snapshot.Revision, "routes", len(snapshot.Routes))
	}
}
