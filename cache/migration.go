package cache

import (
	"context"
	"fmt"
)

func (s *EngineService) Export(ctx context.Context, opts RangeOptions) (Snapshot, error) {
	snapshot, err := s.engine.Export(ctx, opts)
	if err != nil {
		return Snapshot{}, fmt.Errorf("export engine range: %w", err)
	}
	return snapshot, nil
}

func (s *EngineService) Import(ctx context.Context, snapshot Snapshot) (ImportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.engine.Import(ctx, snapshot)
	if err != nil {
		return ImportResult{}, fmt.Errorf("import engine snapshot: %w", err)
	}
	return result, nil
}

func (s *EngineService) DeleteRange(ctx context.Context, opts RangeOptions) (DeleteRangeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.engine.DeleteRange(ctx, opts)
	if err != nil {
		return DeleteRangeResult{}, fmt.Errorf("delete engine range: %w", err)
	}
	return result, nil
}
