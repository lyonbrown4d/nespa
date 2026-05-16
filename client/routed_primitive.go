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
	results := make([]cachewire.PrimitiveResult, 0, len(request.Items))
	for index := range request.Items {
		result, err := c.Primitive(ctx, request.Items[index])
		if err != nil {
			return cachewire.BatchPrimitiveResponse{Results: results}, fmt.Errorf("batch execute routed cache primitive: %w", err)
		}
		results = append(results, result)
	}
	return cachewire.BatchPrimitiveResponse{Results: results}, nil
}

func stampPrimitiveRequest(request *cachewire.PrimitiveRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
	request.NamespaceVersion = decision.namespaceVersion
	request.SpaceVersion = decision.spaceVersion
}
