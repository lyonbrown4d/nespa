package client

import (
	"context"
	"fmt"
	"sort"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/samber/oops"
)

type indexedSetRequest struct {
	index   int
	request cachewire.SetRequest
}

type indexedGetRequest struct {
	index   int
	request cachewire.GetRequest
}

type routedBatchGroup[T any] struct {
	addr  string
	items *collectionlist.List[T]
}

func (c *RoutedTCPClient) BatchSet(ctx context.Context, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	records, err := routedBatch(ctx, c, len(request.Items),
		indexSetRequests(request.Items),
		groupSetRequests,
		func(epoch uint64, addr string, group *collectionlist.List[indexedSetRequest]) ([]cachewire.Record, error) {
			response, sendErr := c.transport.BatchSet(ctx, addr, cachewire.BatchSetRequest{
				RouteEpoch: epoch,
				Items:      setItems(group),
			})
			if sendErr != nil {
				return nil, fmt.Errorf("batch set routed cache records: %w", sendErr)
			}
			return response.Records, nil
		},
		copySetRecords,
	)
	if err != nil {
		return cachewire.BatchSetResponse{}, err
	}
	return cachewire.BatchSetResponse{Records: records}, nil
}

func (c *RoutedTCPClient) BatchGet(ctx context.Context, request cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error) {
	records, err := routedBatch(ctx, c, len(request.Items),
		indexGetRequests(request.Items),
		groupGetRequests,
		func(epoch uint64, addr string, group *collectionlist.List[indexedGetRequest]) ([]cachewire.Record, error) {
			response, sendErr := c.transport.BatchGet(ctx, addr, cachewire.BatchGetRequest{
				RouteEpoch: epoch,
				Items:      getItems(group),
			})
			if sendErr != nil {
				return nil, fmt.Errorf("batch get routed cache records: %w", sendErr)
			}
			return response.Records, nil
		},
		copyGetRecords,
	)
	if err != nil {
		return cachewire.BatchGetResponse{}, err
	}
	return cachewire.BatchGetResponse{Records: records}, nil
}

func indexSetRequests(items []cachewire.SetRequest) *collectionlist.List[indexedSetRequest] {
	indexed := collectionlist.NewListWithCapacity[indexedSetRequest](len(items))
	for index := range items {
		indexed.Add(indexedSetRequest{
			index:   index,
			request: items[index],
		})
	}
	return indexed
}

func indexGetRequests(items []cachewire.GetRequest) *collectionlist.List[indexedGetRequest] {
	indexed := collectionlist.NewListWithCapacity[indexedGetRequest](len(items))
	for index := range items {
		indexed.Add(indexedGetRequest{
			index:   index,
			request: items[index],
		})
	}
	return indexed
}

func routedBatch[T any](
	ctx context.Context,
	client *RoutedTCPClient,
	count int,
	items *collectionlist.List[T],
	group func(controlapi.SnapshotBody, *collectionlist.List[T]) (*collectionlist.List[routedBatchGroup[T]], error),
	send func(uint64, string, *collectionlist.List[T]) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, *collectionlist.List[T], []cachewire.Record),
) ([]cachewire.Record, error) {
	if count == 0 {
		return nil, nil
	}
	records, remaining, err := runRoutedBatchWithCurrentSnapshot(ctx, client, count, items, group, send, copyRecords)
	if err == nil {
		return records, nil
	}
	refresh, _ := shouldRefreshRoute(err)
	if !refresh {
		return records, err
	}
	if refreshErr := client.Refresh(ctx); refreshErr != nil {
		return nil, oops.Code("route_snapshot_refresh_failed").
			In("client.routing").
			Wrap(refreshErr)
	}
	snapshot, snapshotErr := client.currentSnapshot(ctx)
	if snapshotErr != nil {
		return nil, oops.Code("route_snapshot_read_failed").
			In("client.routing").
			Wrap(snapshotErr)
	}
	retryItems := flattenBatchGroups(remaining)
	groups, err := group(snapshot, retryItems)
	if err != nil {
		return records, err
	}
	records, _, err = runRoutedBatch(count, snapshot.Revision, groups, records, send, copyRecords)
	return records, err
}

func runRoutedBatchWithCurrentSnapshot[T any](
	ctx context.Context,
	client *RoutedTCPClient,
	count int,
	items *collectionlist.List[T],
	group func(controlapi.SnapshotBody, *collectionlist.List[T]) (*collectionlist.List[routedBatchGroup[T]], error),
	send func(uint64, string, *collectionlist.List[T]) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, *collectionlist.List[T], []cachewire.Record),
) ([]cachewire.Record, *collectionlist.List[routedBatchGroup[T]], error) {
	snapshot, err := client.currentSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	groups, err := group(snapshot, items)
	if err != nil {
		return nil, nil, err
	}
	records := make([]cachewire.Record, count)
	return runRoutedBatch(count, snapshot.Revision, groups, records, send, copyRecords)
}

func runRoutedBatch[T any](
	count int,
	epoch uint64,
	groups *collectionlist.List[routedBatchGroup[T]],
	records []cachewire.Record,
	send func(uint64, string, *collectionlist.List[T]) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, *collectionlist.List[T], []cachewire.Record),
) ([]cachewire.Record, *collectionlist.List[routedBatchGroup[T]], error) {
	if len(records) != count {
		records = make([]cachewire.Record, count)
	}

	var remaining *collectionlist.List[routedBatchGroup[T]]
	var runErr error
	groups.Range(func(index int, group routedBatchGroup[T]) bool {
		response, err := send(epoch, group.addr, group.items)
		if err != nil {
			remaining = groups.Drop(index)
			runErr = err
			return false
		}
		copyRecords(records, group.items, response)
		return true
	})
	return records, remaining, runErr
}

func groupSetRequests(snapshot controlapi.SnapshotBody, items *collectionlist.List[indexedSetRequest]) (*collectionlist.List[routedBatchGroup[indexedSetRequest]], error) {
	groups := collectionmapping.NewMap[string, *collectionlist.List[indexedSetRequest]]()
	var groupErr error
	items.Range(func(_ int, item indexedSetRequest) bool {
		decision, err := resolveSnapshot(snapshot, item.request.Key)
		if err != nil {
			groupErr = err
			return false
		}
		stampSetRequest(&item.request, decision)
		addBatchGroup(groups, decision.addr, item)
		return true
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return orderedBatchGroups(groups), nil
}

func groupGetRequests(snapshot controlapi.SnapshotBody, items *collectionlist.List[indexedGetRequest]) (*collectionlist.List[routedBatchGroup[indexedGetRequest]], error) {
	groups := collectionmapping.NewMap[string, *collectionlist.List[indexedGetRequest]]()
	var groupErr error
	items.Range(func(_ int, item indexedGetRequest) bool {
		decision, err := resolveSnapshot(snapshot, item.request.Key)
		if err != nil {
			groupErr = err
			return false
		}
		stampGetRequest(&item.request, decision)
		addBatchGroup(groups, decision.addr, item)
		return true
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return orderedBatchGroups(groups), nil
}

func addBatchGroup[T any](groups *collectionmapping.Map[string, *collectionlist.List[T]], addr string, item T) {
	items, ok := groups.Get(addr)
	if !ok {
		items = collectionlist.NewList[T]()
		groups.Set(addr, items)
	}
	items.Add(item)
}

func orderedBatchGroups[T any](groups *collectionmapping.Map[string, *collectionlist.List[T]]) *collectionlist.List[routedBatchGroup[T]] {
	addrs := groups.Keys()
	sort.Strings(addrs)

	out := collectionlist.NewListWithCapacity[routedBatchGroup[T]](len(addrs))
	for _, addr := range addrs {
		items, _ := groups.Get(addr)
		out.Add(routedBatchGroup[T]{
			addr:  addr,
			items: items,
		})
	}
	return out
}

func flattenBatchGroups[T any](groups *collectionlist.List[routedBatchGroup[T]]) *collectionlist.List[T] {
	items := collectionlist.NewList[T]()
	groups.Range(func(_ int, group routedBatchGroup[T]) bool {
		items.Merge(group.items)
		return true
	})
	return items
}

func setItems(group *collectionlist.List[indexedSetRequest]) []cachewire.SetRequest {
	items := collectionlist.NewListWithCapacity[cachewire.SetRequest](group.Len())
	group.Range(func(_ int, item indexedSetRequest) bool {
		items.Add(item.request)
		return true
	})
	return items.Values()
}

func getItems(group *collectionlist.List[indexedGetRequest]) []cachewire.GetRequest {
	items := collectionlist.NewListWithCapacity[cachewire.GetRequest](group.Len())
	group.Range(func(_ int, item indexedGetRequest) bool {
		items.Add(item.request)
		return true
	})
	return items.Values()
}

func copySetRecords(records []cachewire.Record, group *collectionlist.List[indexedSetRequest], response []cachewire.Record) {
	for index := range response {
		item, ok := group.Get(index)
		if ok {
			records[item.index] = response[index]
		}
	}
}

func copyGetRecords(records []cachewire.Record, group *collectionlist.List[indexedGetRequest], response []cachewire.Record) {
	for index := range response {
		item, ok := group.Get(index)
		if ok {
			records[item.index] = response[index]
		}
	}
}
