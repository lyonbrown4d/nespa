package frontend

import (
	"context"
	"log/slog"
	"time"
)

type Config struct {
	Addr        string
	ControlAddr string
}

type ServiceRuntime struct {
	cfg           Config
	routeCache    *RouteCache
	controlClient *ControlClient
}

func NewServiceRuntime(cfg Config) *ServiceRuntime {
	return &ServiceRuntime{
		cfg:           cfg,
		routeCache:    NewRouteCache(cfg.ControlAddr, nil),
		controlClient: NewControlClient(cfg.ControlAddr),
	}
}

func StartRouteRefresh(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	if !hasAddress(svc.cfg.ControlAddr) {
		return nil
	}
	refreshRoutes(ctx, logger, svc.controlClient, svc.routeCache, svc.cfg.ControlAddr)
	go runRouteRefreshLoop(ctx, logger, svc.controlClient, svc.routeCache, svc.cfg.ControlAddr, 2*time.Second)
	return nil
}

func (s *ServiceRuntime) Routes() RoutesBody {
	return s.routeCache.Snapshot()
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
