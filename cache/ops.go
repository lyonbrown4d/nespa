package cache

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func (s *EngineService) Stats(ctx context.Context) (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.statsLocked(ctx)
}

func (s *EngineService) statsLocked(ctx context.Context) (Stats, error) {
	stats, err := s.engine.Stats(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("read engine stats: %w", err)
	}
	return stats, nil
}

func (s *EngineService) Evict(ctx context.Context, request EvictRequest) (EvictResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.evictLocked(ctx, request)
}

func (s *EngineService) evictLocked(ctx context.Context, request EvictRequest) (EvictResult, error) {
	result, err := s.engine.Evict(ctx, engine.EvictOptions{
		Namespace:     request.Namespace,
		Space:         request.Space,
		TargetBytes:   request.TargetBytes,
		Exclude:       request.Exclude,
		ExcludeActive: request.Exclude.Key != "",
	})
	if err != nil {
		return EvictResult{}, fmt.Errorf("evict engine records: %w", err)
	}
	return result, nil
}
