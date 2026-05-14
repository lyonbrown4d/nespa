package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

var (
	ErrNoRoute        = errors.New("client: no route")
	ErrCatalogVersion = errors.New("client: catalog version not found")
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
	decision, err := c.resolve(ctx, request.Key)
	if err != nil {
		return cachewire.Record{}, err
	}
	stampSetRequest(&request, decision)
	record, err := c.transport.Set(ctx, decision.addr, request)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("set routed cache record: %w", err)
	}
	return record, nil
}

func (c *RoutedTCPClient) Get(ctx context.Context, request cachewire.GetRequest) (cachewire.Record, error) {
	decision, err := c.resolve(ctx, request.Key)
	if err != nil {
		return cachewire.Record{}, err
	}
	stampGetRequest(&request, decision)
	record, err := c.transport.Get(ctx, decision.addr, request)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("get routed cache record: %w", err)
	}
	return record, nil
}

func (c *RoutedTCPClient) Delete(ctx context.Context, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	decision, err := c.resolve(ctx, request.Key)
	if err != nil {
		return cachewire.DeleteResponse{}, err
	}
	request.RouteEpoch = decision.routeEpoch
	response, err := c.transport.Delete(ctx, decision.addr, request)
	if err != nil {
		return cachewire.DeleteResponse{}, fmt.Errorf("delete routed cache record: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) Exists(ctx context.Context, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	decision, err := c.resolve(ctx, request.Key)
	if err != nil {
		return cachewire.ExistsResponse{}, err
	}
	stampExistsRequest(&request, decision)
	response, err := c.transport.Exists(ctx, decision.addr, request)
	if err != nil {
		return cachewire.ExistsResponse{}, fmt.Errorf("check routed cache record: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) Touch(ctx context.Context, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	decision, err := c.resolve(ctx, request.Key)
	if err != nil {
		return cachewire.TouchResponse{}, err
	}
	stampTouchRequest(&request, decision)
	response, err := c.transport.Touch(ctx, decision.addr, request)
	if err != nil {
		return cachewire.TouchResponse{}, fmt.Errorf("touch routed cache record: %w", err)
	}
	return response, nil
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
		return routeDecision{}, fmt.Errorf("%w: %s/%s/%s", ErrNoRoute, key.Namespace, key.Space, key.Key)
	}
	namespaceVersion, ok := routing.NamespaceVersion(snapshot.Namespaces, key.Namespace)
	if !ok {
		return routeDecision{}, fmt.Errorf("%w: namespace %s", ErrCatalogVersion, key.Namespace)
	}
	spaceVersion, ok := routing.SpaceVersion(snapshot.Spaces, key.Namespace, key.Space)
	if !ok {
		return routeDecision{}, fmt.Errorf("%w: space %s/%s", ErrCatalogVersion, key.Namespace, key.Space)
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
