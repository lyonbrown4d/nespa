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

type serviceRuntime struct {
	cfg           Config
	routeCache    *RouteCache
	controlClient *ControlClient
	nodeClient    *NodeClient
}

func Module() dix.Module {
	return dix.NewModule("frontend",
		dix.WithModuleProviders(
			dix.Provider1(newServiceRuntime),
		),
		dix.WithModuleImports(
			runtime.ConfiguredHTTPModule[*serviceRuntime]("frontend", frontendHTTPConfig),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *serviceRuntime](func(ctx context.Context, logger *slog.Logger, svc *serviceRuntime) error {
				if !hasAddress(svc.cfg.ControlAddr) {
					return nil
				}
				refreshRoutes(ctx, logger, svc.controlClient, svc.routeCache, svc.cfg.ControlAddr)
				go runRouteRefreshLoop(ctx, logger, svc.controlClient, svc.routeCache, svc.cfg.ControlAddr, 2*time.Second)
				return nil
			}, dix.LifecycleName("frontend.routes.refresh"), dix.LifecycleBefore("frontend.http.start")),
		),
	)
}

func newServiceRuntime(cfg Config) *serviceRuntime {
	initialRoutes := []Route{}
	if hasAddress(cfg.NodeAddr) {
		initialRoutes = append(initialRoutes, Route{Role: "data-node", Addr: cfg.NodeAddr, Weight: 1})
	}

	return &serviceRuntime{
		cfg:           cfg,
		routeCache:    NewRouteCache("bootstrap", initialRoutes),
		controlClient: NewControlClient(cfg.ControlAddr),
		nodeClient:    NewNodeClient(),
	}
}

func frontendHTTPConfig(svc *serviceRuntime) runtime.HTTPConfig {
	cfg := svc.cfg
	return runtime.HTTPConfig{
		Name: "frontend",
		Addr: cfg.Addr,
		Metadata: map[string]string{
			"control_addr": cfg.ControlAddr,
			"node_addr":    cfg.NodeAddr,
			"role":         "gateway",
		},
		Routes: func(server httpx.ServerRuntime) {
			registerFrontendRoutes(server, svc)
		},
	}
}

func registerFrontendRoutes(server httpx.ServerRuntime, svc *serviceRuntime) {
	httpx.MustGet(server, "/v1/frontend/routes", frontendRoutesHandler(svc))
	httpx.MustPut(server, "/v1/cache", frontendSetHandler(svc))
	httpx.MustGet(server, "/v1/cache", frontendGetHandler(svc))
	httpx.MustDelete(server, "/v1/cache", frontendDeleteHandler(svc))
}

func frontendRoutesHandler(svc *serviceRuntime) func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[RoutesBody], error) {
	return func(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[RoutesBody], error) {
		return runtime.JSON(svc.routeCache.Snapshot()), nil
	}
}

func frontendSetHandler(svc *serviceRuntime) func(context.Context, *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
	return func(ctx context.Context, input *cacheapi.SetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
		route, ok := svc.routeCache.Select(input.Body.Namespace, input.Body.Space)
		if !ok {
			return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
		}
		rec, err := svc.nodeClient.Set(ctx, route.Addr, input.Body)
		if err != nil {
			return nil, err
		}
		return runtime.JSON(rec), nil
	}
}

func frontendGetHandler(svc *serviceRuntime) func(context.Context, *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
	return func(ctx context.Context, input *cacheapi.GetInput) (*runtime.JSONResponse[cacheapi.RecordBody], error) {
		route, ok := svc.routeCache.Select(input.Namespace, input.Space)
		if !ok {
			return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
		}
		rec, err := svc.nodeClient.Get(ctx, route.Addr, *input)
		if err != nil {
			return nil, err
		}
		return runtime.JSON(rec), nil
	}
}

func frontendDeleteHandler(svc *serviceRuntime) func(context.Context, *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
	return func(ctx context.Context, input *cacheapi.DeleteInput) (*runtime.JSONResponse[cacheapi.DeleteBody], error) {
		route, ok := svc.routeCache.Select(input.Namespace, input.Space)
		if !ok {
			return nil, httpx.NewError(http.StatusServiceUnavailable, "no data-node route available")
		}
		out, err := svc.nodeClient.Delete(ctx, route.Addr, *input)
		if err != nil {
			return nil, err
		}
		return runtime.JSON(out), nil
	}
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
