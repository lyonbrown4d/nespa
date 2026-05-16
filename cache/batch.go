package cache

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func (s *EngineService) BatchSet(ctx context.Context, requests []SetRequest) ([]SetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]SetResult, 0, len(requests))
	for _, request := range requests {
		if err := s.admitSet(ctx, request.Key, request.Value, request.Options); err != nil {
			return records, err
		}
		record, applied, err := s.engine.Set(ctx, request.Key, request.Value, engine.SetOptions{
			TTL:              request.Options.TTL,
			NamespaceVersion: request.Options.NamespaceVersion,
			SpaceVersion:     request.Options.SpaceVersion,
			ExpectedVersion:  request.Options.ExpectedVersion,
		})
		if err != nil {
			return records, fmt.Errorf("set engine batch record: %w", err)
		}
		records = append(records, SetResult{Record: record, Found: applied})
	}
	return records, nil
}

func (s *EngineService) BatchGet(ctx context.Context, requests []GetRequest) ([]GetResult, error) {
	results := make([]GetResult, 0, len(requests))
	for _, request := range requests {
		record, found, err := s.Get(ctx, request.Key, request.Options)
		if err != nil {
			return results, err
		}
		results = append(results, GetResult{Record: record, Found: found})
	}
	return results, nil
}

func (s *EngineService) BatchDelete(ctx context.Context, requests []DeleteRequest) ([]DeleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]DeleteResult, 0, len(requests))
	for index := range requests {
		deleted, found, err := s.engine.Delete(ctx, requests[index].Key, engine.DeleteOptions{
			ExpectedVersion: requests[index].ExpectedVersion,
		})
		if err != nil {
			return results, fmt.Errorf("delete engine batch record: %w", err)
		}
		results = append(results, DeleteResult{Deleted: deleted && found, Found: found})
	}
	return results, nil
}

func (s *EngineService) BatchExists(ctx context.Context, requests []GetRequest) ([]ExistsResult, error) {
	results := make([]ExistsResult, 0, len(requests))
	for index := range requests {
		exists, err := s.Exists(ctx, requests[index].Key, requests[index].Options)
		if err != nil {
			return results, err
		}
		results = append(results, ExistsResult{Exists: exists})
	}
	return results, nil
}

func (s *EngineService) BatchTouch(ctx context.Context, requests []TouchRequest) ([]TouchResult, error) {
	results := make([]TouchResult, 0, len(requests))
	for index := range requests {
		touched, err := s.Touch(ctx, requests[index].Key, requests[index].Options)
		if err != nil {
			return results, err
		}
		results = append(results, TouchResult{Touched: touched})
	}
	return results, nil
}

func (s *EngineService) BatchPrimitive(
	ctx context.Context,
	requests []PrimitiveRequest,
) ([]PrimitiveResult, error) {
	if primitiveBatchMutates(requests) {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	results := make([]PrimitiveResult, 0, len(requests))
	for index := range requests {
		if err := s.admitPrimitive(ctx, requests[index]); err != nil {
			return results, err
		}
		result, err := s.executePrimitive(ctx, requests[index])
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func primitiveBatchMutates(requests []PrimitiveRequest) bool {
	for index := range requests {
		if requests[index].Kind.Mutates() {
			return true
		}
	}
	return false
}
