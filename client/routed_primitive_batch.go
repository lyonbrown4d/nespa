package client

import (
	"context"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type indexedPrimitiveRequest struct {
	index   int
	request cachewire.PrimitiveRequest
}

func (c *RoutedTCPClient) batchPrimitive(
	ctx context.Context,
	request cachewire.BatchPrimitiveRequest,
) (cachewire.BatchPrimitiveResponse, error) {
	results, err := routedBatch[indexedPrimitiveRequest, cachewire.PrimitiveResult](
		ctx,
		c,
		len(request.Items),
		indexPrimitiveRequests(request.Items),
		groupPrimitiveRequests,
		c.sendPrimitiveBatch(ctx),
		copyPrimitiveResults,
	)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{Results: results}, err
	}
	return cachewire.BatchPrimitiveResponse{Results: results}, nil
}

func (c *RoutedTCPClient) sendPrimitiveBatch(
	ctx context.Context,
) func(uint64, string, *collectionlist.List[indexedPrimitiveRequest]) ([]cachewire.PrimitiveResult, error) {
	return func(epoch uint64, addr string, group *collectionlist.List[indexedPrimitiveRequest]) ([]cachewire.PrimitiveResult, error) {
		response, err := c.transport.BatchPrimitive(ctx, addr, cachewire.BatchPrimitiveRequest{
			RouteEpoch: epoch,
			Items:      primitiveItems(group),
		})
		if err != nil {
			return nil, fmt.Errorf("batch primitive routed cache records: %w", err)
		}
		return response.Results, nil
	}
}

func indexPrimitiveRequests(items []cachewire.PrimitiveRequest) *collectionlist.List[indexedPrimitiveRequest] {
	indexed := collectionlist.NewListWithCapacity[indexedPrimitiveRequest](len(items))
	for index := range items {
		indexed.Add(indexedPrimitiveRequest{
			index:   index,
			request: items[index],
		})
	}
	return indexed
}

func groupPrimitiveRequests(
	snapshot controlapi.SnapshotBody,
	items *collectionlist.List[indexedPrimitiveRequest],
) (*collectionlist.List[routedBatchGroup[indexedPrimitiveRequest]], error) {
	groups := collectionmapping.NewMap[string, *collectionlist.List[indexedPrimitiveRequest]]()
	var groupErr error
	items.Range(func(_ int, item indexedPrimitiveRequest) bool {
		decision, err := resolveSnapshot(snapshot, item.request.Key)
		if err != nil {
			groupErr = err
			return false
		}
		stampPrimitiveRequest(&item.request, decision)
		addBatchGroup(groups, decision.addr, item)
		return true
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return orderedBatchGroups(groups), nil
}

func primitiveItems(group *collectionlist.List[indexedPrimitiveRequest]) []cachewire.PrimitiveRequest {
	items := collectionlist.NewListWithCapacity[cachewire.PrimitiveRequest](group.Len())
	group.Range(func(_ int, item indexedPrimitiveRequest) bool {
		items.Add(item.request)
		return true
	})
	return items.Values()
}

func copyPrimitiveResults(
	results []cachewire.PrimitiveResult,
	group *collectionlist.List[indexedPrimitiveRequest],
	response []cachewire.PrimitiveResult,
) {
	for index := range response {
		item, ok := group.Get(index)
		if ok {
			results[item.index] = response[index]
		}
	}
}
