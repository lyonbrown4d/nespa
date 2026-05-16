package client

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func indexBatchItems[T any](items []T) *collectionlist.List[indexedBatchItem[T]] {
	indexed := collectionlist.NewListWithCapacity[indexedBatchItem[T]](len(items))
	for index := range items {
		indexed.Add(indexedBatchItem[T]{index: index, request: items[index]})
	}
	return indexed
}

func groupRoutedWireRequests[T any](
	snapshot controlapi.SnapshotBody,
	items *collectionlist.List[indexedBatchItem[T]],
	keyOf func(T) cachewire.Key,
	stamp func(*T, routeDecision),
) (*collectionlist.List[routedBatchGroup[indexedBatchItem[T]]], error) {
	groups := collectionmapping.NewMap[string, *collectionlist.List[indexedBatchItem[T]]]()
	var groupErr error
	items.Range(func(_ int, item indexedBatchItem[T]) bool {
		decision, err := resolveSnapshot(snapshot, keyOf(item.request))
		if err != nil {
			groupErr = err
			return false
		}
		stamp(&item.request, decision)
		addBatchGroup(groups, decision.addr, item)
		return true
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return orderedBatchGroups(groups), nil
}

func batchItems[T any](group *collectionlist.List[indexedBatchItem[T]]) []T {
	items := collectionlist.NewListWithCapacity[T](group.Len())
	group.Range(func(_ int, item indexedBatchItem[T]) bool {
		items.Add(item.request)
		return true
	})
	return items.Values()
}

func copyBatchResults[R, T any](results []R, group *collectionlist.List[indexedBatchItem[T]], response []R) {
	for index := range response {
		item, ok := group.Get(index)
		if ok {
			results[item.index] = response[index]
		}
	}
}

func deleteRequestKey(request cachewire.DeleteRequest) cachewire.Key {
	return request.Key
}

func existsRequestKey(request cachewire.ExistsRequest) cachewire.Key {
	return request.Key
}

func touchRequestKey(request cachewire.TouchRequest) cachewire.Key {
	return request.Key
}

func stampDeleteRequest(request *cachewire.DeleteRequest, decision routeDecision) {
	request.RouteEpoch = decision.routeEpoch
}
