package tcp

import (
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
)

func primitiveRequestFromWire(request cachewire.PrimitiveRequest) cache.PrimitiveRequest {
	return cache.PrimitiveRequest{
		Kind:         cache.PrimitiveKind(request.Kind),
		Key:          keyFromWire(request.Key),
		Options:      primitiveOptionsFromWire(request),
		Field:        request.Field,
		Member:       request.Member,
		Value:        request.Value,
		Delta:        request.Delta,
		InitialValue: request.InitialValue,
		Score:        request.Score,
		MinScore:     request.MinScore,
		MaxScore:     request.MaxScore,
		HasMinScore:  request.HasMinScore,
		HasMaxScore:  request.HasMaxScore,
		Limit:        request.Limit,
		Start:        request.Start,
		Reverse:      request.Reverse,
	}
}

func primitiveOptionsFromWire(request cachewire.PrimitiveRequest) cache.PrimitiveOptions {
	return cache.PrimitiveOptions{
		TTL:              ttlFromMillis(request.TTLMillis),
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
		ExpectedVersion:  request.ExpectedVersion,
	}
}

func primitiveRequestsFromWire(items []cachewire.PrimitiveRequest) []cache.PrimitiveRequest {
	requests := make([]cache.PrimitiveRequest, 0, len(items))
	for index := range items {
		requests = append(requests, primitiveRequestFromWire(items[index]))
	}
	return requests
}

func primitiveResultFromCache(result cache.PrimitiveResult) cachewire.PrimitiveResult {
	return cachewire.PrimitiveResult{
		Record:        recordFromCache(result.Record, result.Record.Version > 0),
		Found:         result.Found,
		Applied:       result.Applied,
		Value:         result.Value,
		Bool:          result.Bool,
		Count:         result.Count,
		Fields:        mapFieldsFromCache(result.Fields),
		Members:       append([]string(nil), result.Members...),
		ScoredMembers: scoredMembersFromCache(result.ScoredMembers),
		Values:        listValuesFromCache(result.Values),
	}
}

func primitiveResultsFromCache(items []cache.PrimitiveResult) []cachewire.PrimitiveResult {
	results := make([]cachewire.PrimitiveResult, 0, len(items))
	for index := range items {
		results = append(results, primitiveResultFromCache(items[index]))
	}
	return results
}

func mapFieldsFromCache(items []cache.MapField) []cachewire.MapField {
	fields := make([]cachewire.MapField, 0, len(items))
	for index := range items {
		fields = append(fields, cachewire.MapField{
			Field: items[index].Field,
			Value: items[index].Value,
		})
	}
	return fields
}

func scoredMembersFromCache(items []cache.ScoredMember) []cachewire.ScoredMember {
	members := make([]cachewire.ScoredMember, 0, len(items))
	for index := range items {
		members = append(members, cachewire.ScoredMember{
			Member: items[index].Member,
			Score:  items[index].Score,
		})
	}
	return members
}

func listValuesFromCache(items [][]byte) []cachewire.ListValue {
	values := make([]cachewire.ListValue, 0, len(items))
	for index := range items {
		values = append(values, cachewire.ListValue{Value: items[index]})
	}
	return values
}
