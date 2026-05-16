package client

import (
	"context"
	"fmt"
	"sort"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
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
	items []T
}

func (c *RoutedTCPClient) BatchSet(ctx context.Context, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	items := make([]indexedSetRequest, 0, len(request.Items))
	for index := range request.Items {
		items = append(items, indexedSetRequest{
			index:   index,
			request: request.Items[index],
		})
	}

	records, err := routedBatch(ctx, c, len(request.Items),
		items,
		func(snapshot controlapi.SnapshotBody, items []indexedSetRequest) ([]routedBatchGroup[indexedSetRequest], error) {
			return groupSetRequests(snapshot, items)
		},
		func(epoch uint64, addr string, group []indexedSetRequest) ([]cachewire.Record, error) {
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
	items := make([]indexedGetRequest, 0, len(request.Items))
	for index := range request.Items {
		items = append(items, indexedGetRequest{
			index:   index,
			request: request.Items[index],
		})
	}

	records, err := routedBatch(ctx, c, len(request.Items),
		items,
		func(snapshot controlapi.SnapshotBody, items []indexedGetRequest) ([]routedBatchGroup[indexedGetRequest], error) {
			return groupGetRequests(snapshot, items)
		},
		func(epoch uint64, addr string, group []indexedGetRequest) ([]cachewire.Record, error) {
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

func routedBatch[T any](
	ctx context.Context,
	client *RoutedTCPClient,
	count int,
	items []T,
	group func(controlapi.SnapshotBody, []T) ([]routedBatchGroup[T], error),
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, error) {
	if count == 0 {
		return nil, nil
	}
	records, remaining, err := runRoutedBatchWithCurrentSnapshot(ctx, client, count, items, group, send, copyRecords)
	if err == nil || !isWireNoRoute(err) {
		return records, err
	}
	if refreshErr := client.Refresh(ctx); refreshErr != nil {
		return nil, fmt.Errorf("refresh routed cache snapshot: %w", refreshErr)
	}
	snapshot, snapshotErr := client.currentSnapshot(ctx)
	if snapshotErr != nil {
		return nil, fmt.Errorf("routed cache snapshot: %w", snapshotErr)
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
	items []T,
	group func(controlapi.SnapshotBody, []T) ([]routedBatchGroup[T], error),
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, []routedBatchGroup[T], error) {
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
	groups []routedBatchGroup[T],
	records []cachewire.Record,
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, []routedBatchGroup[T], error) {
	if len(records) != count {
		records = make([]cachewire.Record, count)
	}

	for index := range groups {
		group := groups[index]
		response, err := send(epoch, group.addr, group.items)
		if err != nil {
			return records, groups[index:], err
		}
		copyRecords(records, group.items, response)
	}
	return records, nil, nil
}

func groupSetRequests(snapshot controlapi.SnapshotBody, items []indexedSetRequest) ([]routedBatchGroup[indexedSetRequest], error) {
	groups := make(map[string][]indexedSetRequest)
	for index := range items {
		item := items[index]
		decision, err := resolveSnapshot(snapshot, item.request.Key)
		if err != nil {
			return nil, err
		}
		stampSetRequest(&item.request, decision)
		groups[decision.addr] = append(groups[decision.addr], item)
	}
	return orderedBatchGroups(groups), nil
}

func groupGetRequests(snapshot controlapi.SnapshotBody, items []indexedGetRequest) ([]routedBatchGroup[indexedGetRequest], error) {
	groups := make(map[string][]indexedGetRequest)
	for index := range items {
		item := items[index]
		decision, err := resolveSnapshot(snapshot, item.request.Key)
		if err != nil {
			return nil, err
		}
		stampGetRequest(&item.request, decision)
		groups[decision.addr] = append(groups[decision.addr], item)
	}
	return orderedBatchGroups(groups), nil
}

func orderedBatchGroups[T any](groups map[string][]T) []routedBatchGroup[T] {
	addrs := make([]string, 0, len(groups))
	for addr := range groups {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)

	out := make([]routedBatchGroup[T], 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, routedBatchGroup[T]{
			addr:  addr,
			items: groups[addr],
		})
	}
	return out
}

func flattenBatchGroups[T any](groups []routedBatchGroup[T]) []T {
	var items []T
	for index := range groups {
		items = append(items, groups[index].items...)
	}
	return items
}

func setItems(group []indexedSetRequest) []cachewire.SetRequest {
	items := make([]cachewire.SetRequest, 0, len(group))
	for index := range group {
		items = append(items, group[index].request)
	}
	return items
}

func getItems(group []indexedGetRequest) []cachewire.GetRequest {
	items := make([]cachewire.GetRequest, 0, len(group))
	for index := range group {
		items = append(items, group[index].request)
	}
	return items
}

func copySetRecords(records []cachewire.Record, group []indexedSetRequest, response []cachewire.Record) {
	for index := range response {
		if index < len(group) {
			records[group[index].index] = response[index]
		}
	}
}

func copyGetRecords(records []cachewire.Record, group []indexedGetRequest, response []cachewire.Record) {
	for index := range response {
		if index < len(group) {
			records[group[index].index] = response[index]
		}
	}
}
