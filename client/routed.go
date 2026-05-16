package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/protocol"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
	"github.com/samber/oops"
)

var (
	ErrNoRoute        = oops.Code("no_route").In("client.routing").New("client: no route")
	ErrCatalogVersion = oops.Code("catalog_version_not_found").In("client.routing").New("client: catalog version not found")
)

type RoutedConfig struct {
	ControlAddr string
}

type RoutedTCPClient struct {
	control   *controlSnapshotClient
	transport *cachetcp.Client

	mu       sync.RWMutex
	snapshot controlapi.SnapshotBody
}

type routeDecision struct {
	addr             string
	routeEpoch       uint64
	namespaceVersion uint64
	spaceVersion     uint64
}

func NewRoutedTCP(cfg RoutedConfig) (*RoutedTCPClient, error) {
	control, err := newControlSnapshotClient(cfg.ControlAddr)
	if err != nil {
		return nil, err
	}
	return &RoutedTCPClient{
		control:   control,
		transport: cachetcp.NewClient(),
	}, nil
}

func (c *RoutedTCPClient) Refresh(ctx context.Context) error {
	snapshot, err := c.control.Snapshot(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.snapshot = snapshot
	c.mu.Unlock()
	return nil
}

func (c *RoutedTCPClient) Set(ctx context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
	record, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.Record, error) {
		next := request
		stampSetRequest(&next, decision)
		return c.transport.Set(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("set routed cache record: %w", err)
	}
	return record, nil
}

func (c *RoutedTCPClient) Get(ctx context.Context, request cachewire.GetRequest) (cachewire.Record, error) {
	record, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.Record, error) {
		next := request
		stampGetRequest(&next, decision)
		return c.transport.Get(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("get routed cache record: %w", err)
	}
	return record, nil
}

func (c *RoutedTCPClient) Delete(ctx context.Context, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	response, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.DeleteResponse, error) {
		next := request
		next.RouteEpoch = decision.routeEpoch
		return c.transport.Delete(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.DeleteResponse{}, fmt.Errorf("delete routed cache record: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) Exists(ctx context.Context, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	response, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.ExistsResponse, error) {
		next := request
		stampExistsRequest(&next, decision)
		return c.transport.Exists(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.ExistsResponse{}, fmt.Errorf("check routed cache record: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) Touch(ctx context.Context, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	response, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.TouchResponse, error) {
		next := request
		stampTouchRequest(&next, decision)
		return c.transport.Touch(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.TouchResponse{}, fmt.Errorf("touch routed cache record: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) Adjust(ctx context.Context, request cachewire.AdjustRequest) (cachewire.Record, error) {
	response, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.Record, error) {
		next := request
		stampAdjustRequest(&next, decision)
		return c.transport.Adjust(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("adjust routed cache record: %w", err)
	}
	return response, nil
}

func sendWithRouteRetry[T any](
	ctx context.Context,
	client *RoutedTCPClient,
	key cachewire.Key,
	send func(routeDecision) (T, error),
) (T, error) {
	decision, err := client.resolve(ctx, key)
	if err != nil {
		var zero T
		return zero, err
	}
	response, err := send(decision)
	if err == nil {
		return response, nil
	}
	refresh, requireChange := shouldRefreshRoute(err)
	if !refresh {
		return response, err
	}
	if refreshErr := client.Refresh(ctx); refreshErr != nil {
		var zero T
		return zero, oops.Code("route_snapshot_refresh_failed").
			In("client.routing").
			Wrap(refreshErr)
	}
	next, err := client.resolve(ctx, key)
	if err != nil {
		var zero T
		return zero, err
	}
	if requireChange && sameRouteDecision(decision, next) {
		return response, err
	}
	return send(next)
}

func shouldRefreshRoute(err error) (bool, bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}
	var wireErr cachewire.Error
	if !errors.As(err, &wireErr) {
		return true, true
	}
	switch wireErr.Code {
	case protocol.ErrorNoRoute:
		return true, false
	case protocol.ErrorTimeout, protocol.ErrorUnavailable:
		return true, true
	case protocol.ErrorUnknown,
		protocol.ErrorBadFrame,
		protocol.ErrorUnsupportedVersion,
		protocol.ErrorTooLarge,
		protocol.ErrorInternal,
		protocol.ErrorInvalidArgument:
		return false, false
	}
	return false, false
}

func sameRouteDecision(left, right routeDecision) bool {
	return left.addr == right.addr &&
		left.routeEpoch == right.routeEpoch &&
		left.namespaceVersion == right.namespaceVersion &&
		left.spaceVersion == right.spaceVersion
}

func (c *RoutedTCPClient) resolve(ctx context.Context, key cachewire.Key) (routeDecision, error) {
	snapshot, err := c.currentSnapshot(ctx)
	if err != nil {
		return routeDecision{}, err
	}
	return resolveSnapshot(snapshot, key)
}

func (c *RoutedTCPClient) currentSnapshot(ctx context.Context) (controlapi.SnapshotBody, error) {
	c.mu.RLock()
	snapshot := c.snapshot
	c.mu.RUnlock()

	if snapshot.Revision != 0 {
		return snapshot, nil
	}
	if err := c.Refresh(ctx); err != nil {
		return controlapi.SnapshotBody{}, err
	}

	c.mu.RLock()
	snapshot = c.snapshot
	c.mu.RUnlock()
	return snapshot, nil
}

func resolveSnapshot(snapshot controlapi.SnapshotBody, key cachewire.Key) (routeDecision, error) {
	route, ok := routing.Select(snapshot.Routes, key.Namespace, key.Space, key.Key)
	if !ok {
		return routeDecision{}, oops.Code("no_route").
			In("client.routing").
			With("namespace", key.Namespace, "space", key.Space, "key", key.Key).
			Wrap(ErrNoRoute)
	}
	namespaceVersion, ok := routing.NamespaceVersion(snapshot.Namespaces, key.Namespace)
	if !ok {
		return routeDecision{}, oops.Code("catalog_version_not_found").
			In("client.routing").
			With("namespace", key.Namespace).
			Wrap(ErrCatalogVersion)
	}
	spaceVersion, ok := routing.SpaceVersion(snapshot.Spaces, key.Namespace, key.Space)
	if !ok {
		return routeDecision{}, oops.Code("catalog_version_not_found").
			In("client.routing").
			With("namespace", key.Namespace, "space", key.Space).
			Wrap(ErrCatalogVersion)
	}
	return routeDecision{
		addr:             route.Addr,
		routeEpoch:       snapshot.Revision,
		namespaceVersion: namespaceVersion,
		spaceVersion:     spaceVersion,
	}, nil
}

func stampSetRequest(request *cachewire.SetRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}

func stampGetRequest(request *cachewire.GetRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}

func stampExistsRequest(request *cachewire.ExistsRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}

func stampTouchRequest(request *cachewire.TouchRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}

func stampAdjustRequest(request *cachewire.AdjustRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}
