package cache

import (
	"context"
	"fmt"
)

func (s *EngineService) BatchSet(ctx context.Context, requests []SetRequest) ([]SetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.batchSetLocked(ctx, requests)
}

func (s *EngineService) batchSetLocked(ctx context.Context, requests []SetRequest) ([]SetResult, error) {
	records := make([]SetResult, 0, len(requests))
	for _, request := range requests {
		result, err := s.setLocked(ctx, request.Key, request.Value, request.Options)
		if err != nil {
			return records, err
		}
		records = append(records, result)
	}
	return records, nil
}

func (s *EngineService) BatchGet(ctx context.Context, requests []GetRequest) ([]GetResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.batchGetLocked(ctx, requests)
}

func (s *EngineService) batchGetLocked(ctx context.Context, requests []GetRequest) ([]GetResult, error) {
	results := make([]GetResult, 0, len(requests))
	for _, request := range requests {
		record, found, err := s.getLocked(ctx, request.Key, request.Options)
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

	return s.batchDeleteLocked(ctx, requests)
}

func (s *EngineService) batchDeleteLocked(ctx context.Context, requests []DeleteRequest) ([]DeleteResult, error) {
	results := make([]DeleteResult, 0, len(requests))
	for index := range requests {
		deleted, found, err := s.deleteLocked(ctx, requests[index].Key, DeleteOptions{
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.batchExistsLocked(ctx, requests)
}

func (s *EngineService) batchExistsLocked(ctx context.Context, requests []GetRequest) ([]ExistsResult, error) {
	results := make([]ExistsResult, 0, len(requests))
	for index := range requests {
		exists, err := s.existsLocked(ctx, requests[index].Key, requests[index].Options)
		if err != nil {
			return results, err
		}
		results = append(results, ExistsResult{Exists: exists})
	}
	return results, nil
}

func (s *EngineService) BatchTouch(ctx context.Context, requests []TouchRequest) ([]TouchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.batchTouchLocked(ctx, requests)
}

func (s *EngineService) batchTouchLocked(ctx context.Context, requests []TouchRequest) ([]TouchResult, error) {
	results := make([]TouchResult, 0, len(requests))
	for index := range requests {
		touched, err := s.touchLocked(ctx, requests[index].Key, requests[index].Options)
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
		return s.batchPrimitiveLocked(ctx, requests)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.batchPrimitiveLocked(ctx, requests)
}

func (s *EngineService) batchPrimitiveLocked(
	ctx context.Context,
	requests []PrimitiveRequest,
) ([]PrimitiveResult, error) {
	results := make([]PrimitiveResult, 0, len(requests))
	for index := range requests {
		result, err := s.primitiveLocked(ctx, requests[index])
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
