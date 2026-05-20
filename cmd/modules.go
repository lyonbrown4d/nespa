package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/httpx"
	"github.com/arcgolabs/httpx/adapter"
	fiberadapter "github.com/arcgolabs/httpx/adapter/fiber"
	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/frontend"
	"github.com/lyonbrown4d/nespa/node"
	"github.com/lyonbrown4d/nespa/runtime"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func foundationModule(logger *slog.Logger) dix.Module {
	return dix.NewModule("foundation",
		dix.WithModuleProviders(
			dix.Value(logger),
			dix.Provider0(func() eventx.BusRuntime {
				return eventx.New(
					eventx.WithParallelDispatch(true),
					eventx.WithAsyncErrorHandler(func(_ context.Context, event eventx.Event, err error) {
						logger.Warn("event handler failed", "event", event.Name(), "error", err)
					}),
				)
			}),
		),
		dix.WithModuleHooks(
			dix.OnStop[eventx.BusRuntime](func(_ context.Context, bus eventx.BusRuntime) error {
				return bus.Close()
			}, dix.LifecycleName("foundation.eventbus.stop")),
		),
	)
}

type configuredHTTPService[T any, E httpx.Endpoint] struct {
	service *runtime.HTTPService
}

func configuredHTTPModule[T any, E httpx.Endpoint](name string, build func(T) runtime.HTTPConfig) dix.Module {
	return dix.NewModule(name+".http",
		dix.WithModuleProviders(
			dix.Provider2(func(cfg T, endpoints *collectionlist.List[E]) *configuredHTTPService[T, E] {
				httpCfg := build(cfg)
				httpCfg.Endpoints = endpointListValues(endpoints)
				return &configuredHTTPService[T, E]{
					service: runtime.NewHTTPService(httpCfg),
				}
			}),
		),
		dix.WithModuleHooks(
			dix.OnStart3[*configuredHTTPService[T, E], *slog.Logger, eventx.BusRuntime](func(ctx context.Context, service *configuredHTTPService[T, E], logger *slog.Logger, bus eventx.BusRuntime) error {
				return service.service.Start(ctx, logger, bus)
			}, dix.LifecycleName(name+".http.start")),
			dix.OnStop3[*configuredHTTPService[T, E], *slog.Logger, eventx.BusRuntime](func(ctx context.Context, service *configuredHTTPService[T, E], logger *slog.Logger, bus eventx.BusRuntime) error {
				return service.service.Stop(ctx, logger, bus)
			}, dix.LifecycleName(name+".http.stop")),
		),
	)
}

func endpointListValues[E httpx.Endpoint](endpoints *collectionlist.List[E]) []httpx.Endpoint {
	if endpoints == nil {
		return nil
	}

	out := make([]httpx.Endpoint, 0, endpoints.Len())
	endpoints.Range(func(_ int, endpoint E) bool {
		out = append(out, endpoint)
		return true
	})
	return out
}

func controlModule(enabled bool) dix.Module {
	return dix.NewModule("control",
		dix.Disabled(!enabled),
		dix.WithModuleProviders(
			dix.Provider2(control.NewServiceRuntimeWithEvents),
			dix.Contribute1[control.Endpoint, *control.ServiceRuntime](control.NewReadEndpoint, dix.Order(10)),
			dix.Contribute1[control.Endpoint, *control.ServiceRuntime](control.NewCatalogEndpoint, dix.Order(20)),
			dix.Contribute1[control.Endpoint, *control.ServiceRuntime](control.NewNodeEndpoint, dix.Order(30)),
		),
		dix.WithModuleImports(
			configuredHTTPModule[*control.ServiceRuntime, control.Endpoint]("control", control.HTTPConfig),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, eventx.BusRuntime](control.SubscribeRebalanceEvents, dix.LifecycleName("control.rebalance.subscribe"), dix.LifecycleBefore("control.http.start")),
			dix.OnStart2[*slog.Logger, *control.ServiceRuntime](control.RestoreSnapshot, dix.LifecycleName("control.snapshot.restore"), dix.LifecycleBefore("control.dragonboat.start")),
			dix.OnStart2[*slog.Logger, *control.ServiceRuntime](control.StartDragonboat, dix.LifecycleName("control.dragonboat.start"), dix.LifecycleBefore("control.http.start")),
			dix.OnStart2[*slog.Logger, *control.ServiceRuntime](control.StartMigrationExecutor, dix.LifecycleName("control.migration.start"), dix.LifecycleAfter("control.http.start")),
			dix.OnStart2[*slog.Logger, *control.ServiceRuntime](control.StartLiveness, dix.LifecycleName("control.liveness.start"), dix.LifecycleAfter("control.http.start")),
			dix.OnStop2[*slog.Logger, *control.ServiceRuntime](control.SaveSnapshot, dix.LifecycleName("control.snapshot.save")),
			dix.OnStop2[*slog.Logger, *control.ServiceRuntime](control.StopDragonboat, dix.LifecycleName("control.dragonboat.stop"), dix.LifecycleAfter("control.http.stop")),
		),
	)
}

func frontendModule(enabled bool) dix.Module {
	return dix.NewModule("frontend",
		dix.Disabled(!enabled),
		dix.WithModuleProviders(
			dix.Provider1(frontend.NewServiceRuntime),
			dix.ProviderErr2(frontend.NewWebServer),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *frontend.ServiceRuntime](frontend.StartRouteRefresh, dix.LifecycleName("frontend.routes.refresh"), dix.LifecycleBefore("frontend.web.start")),
			dix.OnStart2[*slog.Logger, *frontend.WebServer](func(ctx context.Context, logger *slog.Logger, server *frontend.WebServer) error {
				return server.Start(ctx, logger)
			}, dix.LifecycleName("frontend.web.start")),
			dix.OnStop[*frontend.WebServer](func(ctx context.Context, server *frontend.WebServer) error {
				return server.Stop(ctx)
			}, dix.LifecycleName("frontend.web.stop")),
		),
	)
}

func nodeModule(enabled bool) dix.Module {
	if !enabled {
		return dix.NewModule("node", dix.Disabled(true))
	}

	return dix.NewModule("node",
		dix.WithModuleProviders(
			dix.Provider1(func(node.Config) engine.Engine {
				return engine.NewMemory(engine.Config{
					ShardCount:    16,
					SweepInterval: time.Second,
				})
			}),
			dix.Provider2(func(cfg node.Config, eng engine.Engine) cache.Service {
				return cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{
					DefaultNamespaceMemoryBytes: cfg.DefaultNamespaceMemoryBytes,
					DefaultSpaceMemoryBytes:     cfg.DefaultSpaceMemoryBytes,
				}))
			}),
			dix.Provider2(node.NewServiceRuntime),
			dix.Provider3(func(cfg node.Config, svc cache.Service, nodeSvc *node.ServiceRuntime) *cachetcp.Server {
				return cachetcp.NewServer(cachetcp.ServerConfig{
					Addr:              cfg.Addr,
					CurrentRouteEpoch: nodeSvc.RouteEpoch,
					ReplicaTargets:    nodeSvc.ReplicationTargets,
				}, svc)
			}),
		),
		dix.WithModuleImports(
			engineModule(time.Second),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *cachetcp.Server](func(ctx context.Context, logger *slog.Logger, server *cachetcp.Server) error {
				return server.Start(ctx, logger)
			}, dix.LifecycleName("node.tcp.start")),
			dix.OnStart2[*slog.Logger, *node.ServiceRuntime](node.StartControlRegistration, dix.LifecycleName("node.control.register"), dix.LifecycleAfter("node.tcp.start")),
			dix.OnStop[*cachetcp.Server](func(ctx context.Context, server *cachetcp.Server) error {
				return server.Stop(ctx)
			}, dix.LifecycleName("node.tcp.stop")),
		),
	)
}

func adminModule(enabled bool) dix.Module {
	return dix.NewModule("admin",
		dix.Disabled(!enabled),
		dix.WithModuleProviders(
			dix.Provider4[admin.Endpoint, admin.Config, cache.Service, *control.ServiceRuntime, *node.ServiceRuntime](
				func(
					cfg admin.Config,
					cacheSvc cache.Service,
					controlSvc *control.ServiceRuntime,
					nodeSvc *node.ServiceRuntime,
				) admin.Endpoint {
					return admin.NewSummaryEndpoint(cfg, cacheSvc, controlSvc, nodeSvc)
				},
				dix.Into[admin.Endpoint](dix.Order(10)),
			),
		),
		dix.WithModuleImports(
			configuredHTTPModule[admin.Config, admin.Endpoint]("admin", func(cfg admin.Config) runtime.HTTPConfig {
				httpCfg := admin.HTTPConfig(cfg)
				httpCfg.Adapter = func() adapter.Host {
					return fiberadapter.New(nil)
				}
				return httpCfg
			}),
		),
	)
}

func engineModule(sweepInterval time.Duration) dix.Module {
	if sweepInterval <= 0 {
		sweepInterval = time.Second
	}

	return dix.NewModule("node.engine",
		dix.WithModuleHooks(
			dix.OnStart3[*slog.Logger, node.Config, engine.Engine](node.RestoreEngineSnapshot, dix.LifecycleName("node.engine.snapshot.restore"), dix.LifecycleBefore("node.tcp.start")),
			dix.OnStart3[*slog.Logger, node.Config, engine.Engine](func(ctx context.Context, logger *slog.Logger, cfg node.Config, eng engine.Engine) error {
				if cfg.SnapshotInterval <= 0 || strings.TrimSpace(cfg.SnapshotPath) == "" {
					return nil
				}
				go node.RunSnapshotScheduler(ctx, logger, cfg, eng)
				return nil
			}, dix.LifecycleName("node.engine.snapshot.schedule"), dix.LifecycleAfter("node.engine.snapshot.restore")),
			dix.OnStart[engine.Engine](func(ctx context.Context, eng engine.Engine) error {
				go runSweeper(ctx, eng, sweepInterval)
				return nil
			}, dix.LifecycleName("node.engine.sweeper.start")),
			dix.OnStop3[*slog.Logger, node.Config, engine.Engine](func(ctx context.Context, logger *slog.Logger, cfg node.Config, eng engine.Engine) error {
				if err := node.SaveEngineSnapshot(ctx, logger, cfg, eng); err != nil {
					return fmt.Errorf("save node engine snapshot: %w", err)
				}
				if err := eng.Close(); err != nil {
					return fmt.Errorf("close node engine: %w", err)
				}
				return nil
			}, dix.LifecycleName("node.engine.stop")),
		),
	)
}

func runSweeper(ctx context.Context, eng engine.Engine, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, err := eng.SweepExpired(ctx, now)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, engine.ErrClosed) {
				return
			}
		}
	}
}
