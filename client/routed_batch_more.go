package client

import (
	"context"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type indexedBatchItem[T any] struct {
	index   int
	request T
}

func (c *RoutedTCPClient) BatchDelete(
	ctx context.Context,
	request cachewire.BatchDeleteRequest,
) (cachewire.BatchDeleteResponse, error) {
	results, err := routedBatch[indexedBatchItem[cachewire.DeleteRequest], cachewire.DeleteResponse](
		ctx, c, len(request.Items), indexBatchItems(request.Items), groupDeleteRequests, c.sendDeleteBatch(ctx),
		copyBatchResults[cachewire.DeleteResponse, cachewire.DeleteRequest],
	)
	if err != nil {
		return cachewire.BatchDeleteResponse{Results: results}, err
	}
	return cachewire.BatchDeleteResponse{Results: results}, nil
}

func (c *RoutedTCPClient) BatchExists(
	ctx context.Context,
	request cachewire.BatchExistsRequest,
) (cachewire.BatchExistsResponse, error) {
	results, err := routedBatch[indexedBatchItem[cachewire.ExistsRequest], cachewire.ExistsResponse](
		ctx, c, len(request.Items), indexBatchItems(request.Items), groupExistsRequests, c.sendExistsBatch(ctx),
		copyBatchResults[cachewire.ExistsResponse, cachewire.ExistsRequest],
	)
	if err != nil {
		return cachewire.BatchExistsResponse{Results: results}, err
	}
	return cachewire.BatchExistsResponse{Results: results}, nil
}

func (c *RoutedTCPClient) BatchTouch(
	ctx context.Context,
	request cachewire.BatchTouchRequest,
) (cachewire.BatchTouchResponse, error) {
	results, err := routedBatch[indexedBatchItem[cachewire.TouchRequest], cachewire.TouchResponse](
		ctx, c, len(request.Items), indexBatchItems(request.Items), groupTouchRequests, c.sendTouchBatch(ctx),
		copyBatchResults[cachewire.TouchResponse, cachewire.TouchRequest],
	)
	if err != nil {
		return cachewire.BatchTouchResponse{Results: results}, err
	}
	return cachewire.BatchTouchResponse{Results: results}, nil
}

func (c *RoutedTCPClient) sendDeleteBatch(
	ctx context.Context,
) func(uint64, string, *collectionlist.List[indexedBatchItem[cachewire.DeleteRequest]]) ([]cachewire.DeleteResponse, error) {
	return func(epoch uint64, addr string, group *collectionlist.List[indexedBatchItem[cachewire.DeleteRequest]]) ([]cachewire.DeleteResponse, error) {
		response, err := c.transport.BatchDelete(ctx, addr, cachewire.BatchDeleteRequest{
			RouteEpoch: epoch,
			Items:      batchItems(group),
		})
		if err != nil {
			return nil, fmt.Errorf("batch delete routed cache records: %w", err)
		}
		return response.Results, nil
	}
}

func (c *RoutedTCPClient) sendExistsBatch(
	ctx context.Context,
) func(uint64, string, *collectionlist.List[indexedBatchItem[cachewire.ExistsRequest]]) ([]cachewire.ExistsResponse, error) {
	return func(epoch uint64, addr string, group *collectionlist.List[indexedBatchItem[cachewire.ExistsRequest]]) ([]cachewire.ExistsResponse, error) {
		response, err := c.transport.BatchExists(ctx, addr, cachewire.BatchExistsRequest{
			RouteEpoch: epoch,
			Items:      batchItems(group),
		})
		if err != nil {
			return nil, fmt.Errorf("batch exists routed cache records: %w", err)
		}
		return response.Results, nil
	}
}

func (c *RoutedTCPClient) sendTouchBatch(
	ctx context.Context,
) func(uint64, string, *collectionlist.List[indexedBatchItem[cachewire.TouchRequest]]) ([]cachewire.TouchResponse, error) {
	return func(epoch uint64, addr string, group *collectionlist.List[indexedBatchItem[cachewire.TouchRequest]]) ([]cachewire.TouchResponse, error) {
		response, err := c.transport.BatchTouch(ctx, addr, cachewire.BatchTouchRequest{
			RouteEpoch: epoch,
			Items:      batchItems(group),
		})
		if err != nil {
			return nil, fmt.Errorf("batch touch routed cache records: %w", err)
		}
		return response.Results, nil
	}
}

func groupDeleteRequests(
	snapshot controlapi.SnapshotBody,
	items *collectionlist.List[indexedBatchItem[cachewire.DeleteRequest]],
) (*collectionlist.List[routedBatchGroup[indexedBatchItem[cachewire.DeleteRequest]]], error) {
	return groupRoutedWireRequests(snapshot, items, deleteRequestKey, stampDeleteRequest)
}

func groupExistsRequests(
	snapshot controlapi.SnapshotBody,
	items *collectionlist.List[indexedBatchItem[cachewire.ExistsRequest]],
) (*collectionlist.List[routedBatchGroup[indexedBatchItem[cachewire.ExistsRequest]]], error) {
	return groupRoutedWireRequests(snapshot, items, existsRequestKey, stampExistsRequest)
}

func groupTouchRequests(
	snapshot controlapi.SnapshotBody,
	items *collectionlist.List[indexedBatchItem[cachewire.TouchRequest]],
) (*collectionlist.List[routedBatchGroup[indexedBatchItem[cachewire.TouchRequest]]], error) {
	return groupRoutedWireRequests(snapshot, items, touchRequestKey, stampTouchRequest)
}
