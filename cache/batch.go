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
		if err := s.admitSet(ctx, request.Key, request.Value); err != nil {
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
