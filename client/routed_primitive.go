package client

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func (c *RoutedTCPClient) Primitive(
	ctx context.Context,
	request cachewire.PrimitiveRequest,
) (cachewire.PrimitiveResult, error) {
	response, err := sendWithRouteRetry(ctx, c, request.Key, func(decision routeDecision) (cachewire.PrimitiveResult, error) {
		next := request
		stampPrimitiveRequest(&next, decision)
		return c.transport.Primitive(ctx, decision.addr, next)
	})
	if err != nil {
		return cachewire.PrimitiveResult{}, fmt.Errorf("execute routed cache primitive: %w", err)
	}
	return response, nil
}

func (c *RoutedTCPClient) BatchPrimitive(
	ctx context.Context,
	request cachewire.BatchPrimitiveRequest,
) (cachewire.BatchPrimitiveResponse, error) {
	return c.batchPrimitive(ctx, request)
}

func stampPrimitiveRequest(request *cachewire.PrimitiveRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}
