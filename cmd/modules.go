package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/eventx"
	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/frontend"
	"github.com/lyonbrown4d/nespa/node"
	"github.com/lyonbrown4d/nespa/runtime"
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

func configuredHTTPModule[T any](name string, build func(T) runtime.HTTPConfig) dix.Module {
	var mu sync.Mutex
	var service *runtime.HTTPService

	return dix.NewModule(name+".http",
		dix.WithModuleHooks(
			dix.OnStart3[T, *slog.Logger, eventx.BusRuntime](func(ctx context.Context, cfg T, logger *slog.Logger, bus eventx.BusRuntime) error {
				next := runtime.NewHTTPService(build(cfg))

				mu.Lock()
				service = next
				mu.Unlock()

				return next.Start(ctx, logger, bus)
			}, dix.LifecycleName(name+".http.start")),
			dix.OnStop2[*slog.Logger, eventx.BusRuntime](func(ctx context.Context, logger *slog.Logger, bus eventx.BusRuntime) error {
				mu.Lock()
				current := service
				service = nil
				mu.Unlock()

				if current == nil {
					return nil
				}
				return current.Stop(ctx, logger, bus)
			}, dix.LifecycleName(name+".http.stop")),
		),
	)
}

func controlModule() dix.Module {
	return dix.NewModule("control",
		dix.WithModuleProviders(
			dix.Provider1(control.NewServiceRuntime),
		),
		dix.WithModuleImports(
			configuredHTTPModule[*control.ServiceRuntime]("control", control.HTTPConfig),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *control.ServiceRuntime](control.StartLiveness, dix.LifecycleName("control.liveness.start"), dix.LifecycleAfter("control.http.start")),
		),
	)
}

func frontendModule() dix.Module {
	return dix.NewModule("frontend",
		dix.WithModuleProviders(
			dix.Provider1(frontend.NewServiceRuntime),
		),
		dix.WithModuleImports(
			configuredHTTPModule[*frontend.ServiceRuntime]("frontend", frontend.HTTPConfig),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *frontend.ServiceRuntime](frontend.StartRouteRefresh, dix.LifecycleName("frontend.routes.refresh"), dix.LifecycleBefore("frontend.http.start")),
		),
	)
}

func nodeModule() dix.Module {
	eng := engine.NewMemory(engine.Config{
		ShardCount:    16,
		SweepInterval: time.Second,
	})

	return dix.NewModule("node",
		dix.WithModuleProviders(
			dix.Provider1(func(cfg node.Config) cache.Service {
				return cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{
					DefaultNamespaceMemoryBytes: cfg.DefaultNamespaceMemoryBytes,
					DefaultSpaceMemoryBytes:     cfg.DefaultSpaceMemoryBytes,
				}))
			}),
			dix.Provider2(node.NewServiceRuntime),
		),
		dix.WithModuleImports(
			engineModule(eng, time.Second),
			configuredHTTPModule[*node.ServiceRuntime]("node", node.HTTPConfig),
		),
		dix.WithModuleHooks(
			dix.OnStart2[*slog.Logger, *node.ServiceRuntime](node.StartControlRegistration, dix.LifecycleName("node.control.register"), dix.LifecycleAfter("node.http.start")),
		),
	)
}

func adminModule() dix.Module {
	return configuredHTTPModule[admin.Config]("admin", admin.HTTPConfig)
}

func engineModule(eng engine.Engine, sweepInterval time.Duration) dix.Module {
	if sweepInterval <= 0 {
		sweepInterval = time.Second
	}

	return dix.NewModule("node.engine",
		dix.WithModuleProviders(
			dix.Value[engine.Engine](eng),
		),
		dix.WithModuleHooks(
			dix.OnStart[engine.Engine](func(ctx context.Context, eng engine.Engine) error {
				go runSweeper(ctx, eng, sweepInterval)
				return nil
			}, dix.LifecycleName("node.engine.sweeper.start")),
			dix.OnStop[engine.Engine](func(_ context.Context, eng engine.Engine) error {
				return eng.Close()
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
