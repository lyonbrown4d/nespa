package cache

import (
	"context"
	"fmt"
)

func (s *EngineService) Export(ctx context.Context, opts RangeOptions) (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.exportLocked(ctx, opts)
}

func (s *EngineService) exportLocked(ctx context.Context, opts RangeOptions) (Snapshot, error) {
	snapshot, err := s.engine.Export(ctx, opts)
	if err != nil {
		return Snapshot{}, fmt.Errorf("export engine range: %w", err)
	}
	return snapshot, nil
}

func (s *EngineService) Import(ctx context.Context, snapshot Snapshot) (ImportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.importLocked(ctx, snapshot)
}

func (s *EngineService) importLocked(ctx context.Context, snapshot Snapshot) (ImportResult, error) {
	result, err := s.engine.Import(ctx, snapshot)
	if err != nil {
		return ImportResult{}, fmt.Errorf("import engine snapshot: %w", err)
	}
	return result, nil
}

func (s *EngineService) DeleteRange(ctx context.Context, opts RangeOptions) (DeleteRangeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteRangeLocked(ctx, opts)
}

func (s *EngineService) deleteRangeLocked(ctx context.Context, opts RangeOptions) (DeleteRangeResult, error) {
	result, err := s.engine.DeleteRange(ctx, opts)
	if err != nil {
		return DeleteRangeResult{}, fmt.Errorf("delete engine range: %w", err)
	}
	return result, nil
}
