package nespa

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func (c *Client) Primitive(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	result, err := c.backend.Primitive(ctx, wirePrimitiveRequest(request))
	if err != nil {
		return PrimitiveResult{}, fmt.Errorf("execute nespa primitive: %w", err)
	}
	return primitiveResultFromWire(result), nil
}

func (c *Client) BatchPrimitive(ctx context.Context, requests []PrimitiveRequest) ([]PrimitiveResult, error) {
	response, err := c.backend.BatchPrimitive(ctx, cachewire.BatchPrimitiveRequest{
		Items: wirePrimitiveRequests(requests),
	})
	if err != nil {
		return nil, fmt.Errorf("batch execute nespa primitives: %w", err)
	}
	return primitiveResultsFromWire(response.Results), nil
}

func wirePrimitiveRequests(requests []PrimitiveRequest) []cachewire.PrimitiveRequest {
	out := make([]cachewire.PrimitiveRequest, 0, len(requests))
	for index := range requests {
		out = append(out, wirePrimitiveRequest(requests[index]))
	}
	return out
}

func wirePrimitiveRequest(request PrimitiveRequest) cachewire.PrimitiveRequest {
	return cachewire.PrimitiveRequest{
		Key:              wireKey(request.Key),
		Kind:             request.Kind,
		TTLMillis:        ttlMillis(request.Options.TTL),
		NamespaceVersion: request.Options.NamespaceVersion,
		SpaceVersion:     request.Options.SpaceVersion,
		ExpectedVersion:  request.Options.ExpectedVersion,
		Field:            request.Field,
		Member:           request.Member,
		Value:            append([]byte(nil), request.Value...),
		Delta:            request.Delta,
		InitialValue:     request.InitialValue,
		Score:            request.Score,
		MinScore:         request.MinScore,
		MaxScore:         request.MaxScore,
		HasMinScore:      request.HasMinScore,
		HasMaxScore:      request.HasMaxScore,
		Limit:            request.Limit,
		Start:            request.Start,
		Reverse:          request.Reverse,
	}
}

func primitiveResultsFromWire(results []cachewire.PrimitiveResult) []PrimitiveResult {
	out := make([]PrimitiveResult, 0, len(results))
	for index := range results {
		out = append(out, primitiveResultFromWire(results[index]))
	}
	return out
}

func primitiveResultFromWire(result cachewire.PrimitiveResult) PrimitiveResult {
	return PrimitiveResult{
		Record:        recordFromWire(result.Record),
		Found:         result.Found,
		Applied:       result.Applied,
		Value:         append([]byte(nil), result.Value...),
		Bool:          result.Bool,
		Count:         result.Count,
		Fields:        mapFieldsFromWire(result.Fields),
		Members:       append([]string(nil), result.Members...),
		ScoredMembers: scoredMembersFromWire(result.ScoredMembers),
		Values:        listValuesFromWire(result.Values),
	}
}

func mapFieldsFromWire(fields []cachewire.MapField) []MapField {
	out := make([]MapField, 0, len(fields))
	for index := range fields {
		out = append(out, MapField{
			Field: fields[index].Field,
			Value: append([]byte(nil), fields[index].Value...),
		})
	}
	return out
}

func scoredMembersFromWire(members []cachewire.ScoredMember) []ScoredMember {
	out := make([]ScoredMember, 0, len(members))
	for index := range members {
		out = append(out, ScoredMember{Member: members[index].Member, Score: members[index].Score})
	}
	return out
}

func listValuesFromWire(values []cachewire.ListValue) [][]byte {
	out := make([][]byte, 0, len(values))
	for index := range values {
		out = append(out, append([]byte(nil), values[index].Value...))
	}
	return out
}
