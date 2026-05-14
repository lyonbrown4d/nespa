package client

import (
	"context"
	"fmt"

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

func (c *RoutedTCPClient) BatchSet(ctx context.Context, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	records, err := routedBatch(ctx, c, len(request.Items),
		func(snapshot controlapi.SnapshotBody) (map[string][]indexedSetRequest, error) {
			return groupSetRequests(snapshot, request.Items)
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
	records, err := routedBatch(ctx, c, len(request.Items),
		func(snapshot controlapi.SnapshotBody) (map[string][]indexedGetRequest, error) {
			return groupGetRequests(snapshot, request.Items)
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
	group func(controlapi.SnapshotBody) (map[string][]T, error),
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, error) {
	if count == 0 {
		return nil, nil
	}
	records, err := runRoutedBatchWithCurrentSnapshot(ctx, client, count, group, send, copyRecords)
	if err == nil || !isWireNoRoute(err) {
		return records, err
	}
	if refreshErr := client.Refresh(ctx); refreshErr != nil {
		return nil, fmt.Errorf("refresh routed cache snapshot: %w", refreshErr)
	}
	return runRoutedBatchWithCurrentSnapshot(ctx, client, count, group, send, copyRecords)
}

func runRoutedBatchWithCurrentSnapshot[T any](
	ctx context.Context,
	client *RoutedTCPClient,
	count int,
	group func(controlapi.SnapshotBody) (map[string][]T, error),
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, error) {
	snapshot, err := client.currentSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := group(snapshot)
	if err != nil {
		return nil, err
	}
	return runRoutedBatch(count, snapshot.Revision, groups, send, copyRecords)
}

func groupSetRequests(snapshot controlapi.SnapshotBody, items []cachewire.SetRequest) (map[string][]indexedSetRequest, error) {
	groups := make(map[string][]indexedSetRequest)
	for index := range items {
		item := items[index]
		decision, err := resolveSnapshot(snapshot, item.Key)
		if err != nil {
			return nil, err
		}
		stampSetRequest(&item, decision)
		groups[decision.addr] = append(groups[decision.addr], indexedSetRequest{index: index, request: item})
	}
	return groups, nil
}

func groupGetRequests(snapshot controlapi.SnapshotBody, items []cachewire.GetRequest) (map[string][]indexedGetRequest, error) {
	groups := make(map[string][]indexedGetRequest)
	for index := range items {
		item := items[index]
		decision, err := resolveSnapshot(snapshot, item.Key)
		if err != nil {
			return nil, err
		}
		stampGetRequest(&item, decision)
		groups[decision.addr] = append(groups[decision.addr], indexedGetRequest{index: index, request: item})
	}
	return groups, nil
}

func runRoutedBatch[T any](
	count int,
	epoch uint64,
	groups map[string][]T,
	send func(uint64, string, []T) ([]cachewire.Record, error),
	copyRecords func([]cachewire.Record, []T, []cachewire.Record),
) ([]cachewire.Record, error) {
	records := make([]cachewire.Record, count)
	for addr, group := range groups {
		response, err := send(epoch, addr, group)
		if err != nil {
			return nil, err
		}
		copyRecords(records, group, response)
	}
	return records, nil
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
