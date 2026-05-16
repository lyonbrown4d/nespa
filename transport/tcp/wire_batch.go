package tcp

import (
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
)

func batchDeleteRequests(items []cachewire.DeleteRequest) []cache.DeleteRequest {
	requests := make([]cache.DeleteRequest, 0, len(items))
	for index := range items {
		requests = append(requests, cache.DeleteRequest{
			Key:             keyFromWire(items[index].Key),
			ExpectedVersion: items[index].ExpectedVersion,
		})
	}
	return requests
}

func batchExistsRequests(items []cachewire.ExistsRequest) []cache.GetRequest {
	requests := make([]cache.GetRequest, 0, len(items))
	for index := range items {
		requests = append(requests, cache.GetRequest{
			Key: keyFromWire(items[index].Key),
			Options: cache.GetOptions{
				NamespaceVersion: items[index].NamespaceVersion,
				SpaceVersion:     items[index].SpaceVersion,
			},
		})
	}
	return requests
}

func batchTouchRequests(items []cachewire.TouchRequest) []cache.TouchRequest {
	requests := make([]cache.TouchRequest, 0, len(items))
	for index := range items {
		requests = append(requests, cache.TouchRequest{
			Key:     keyFromWire(items[index].Key),
			Options: touchOptionsFromWire(items[index]),
		})
	}
	return requests
}

func deleteResultsFromCache(results []cache.DeleteResult) []cachewire.DeleteResponse {
	out := make([]cachewire.DeleteResponse, 0, len(results))
	for index := range results {
		out = append(out, cachewire.DeleteResponse{Deleted: results[index].Deleted})
	}
	return out
}

func existsResultsFromCache(results []cache.ExistsResult) []cachewire.ExistsResponse {
	out := make([]cachewire.ExistsResponse, 0, len(results))
	for index := range results {
		out = append(out, cachewire.ExistsResponse{Exists: results[index].Exists})
	}
	return out
}

func touchResultsFromCache(results []cache.TouchResult) []cachewire.TouchResponse {
	out := make([]cachewire.TouchResponse, 0, len(results))
	for index := range results {
		out = append(out, cachewire.TouchResponse{Touched: results[index].Touched})
	}
	return out
}
