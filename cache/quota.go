package cache

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func (s *EngineService) hasQuotaLimits() bool {
	return s.quota.DefaultNamespaceMemoryBytes > 0 || s.quota.DefaultSpaceMemoryBytes > 0 ||
		len(s.quota.Namespaces) > 0 || len(s.quota.Spaces) > 0
}

func (s *EngineService) currentRecord(ctx context.Context, key Key) (Record, bool, error) {
	current, found, err := s.engine.Get(ctx, key, engine.GetOptions{})
	if err != nil {
		return Record{}, false, fmt.Errorf("get engine record for admission: %w", err)
	}
	return current, found, nil
}

func (s *EngineService) admitSet(ctx context.Context, key Key, value []byte, opts SetOptions) error {
	if !s.hasQuotaLimits() {
		return nil
	}

	nextCost := engine.EstimateCost(key, value)
	current, found, err := s.currentRecord(ctx, key)
	if err != nil {
		return err
	}
	if !expectedVersionWillApply(found, current.Version, opts.ExpectedVersion) {
		return nil
	}
	return s.admitCostGrowth(ctx, key, current.CostBytes, nextCost)
}

func (s *EngineService) admitAdjust(ctx context.Context, key Key, opts AdjustOptions) error {
	if !s.hasQuotaLimits() {
		return nil
	}

	estimate, err := s.engine.EstimateAdjust(ctx, key, engine.AdjustOptions{
		Delta:            opts.Delta,
		InitialValue:     opts.InitialValue,
		TTL:              opts.TTL,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return fmt.Errorf("estimate engine adjust for admission: %w", err)
	}
	if !estimate.Applied {
		return nil
	}
	return s.admitCostGrowth(ctx, key, estimate.OldCostBytes, estimate.NewCostBytes)
}

func (s *EngineService) admitPrimitive(ctx context.Context, request PrimitiveRequest) error {
	if !s.hasQuotaLimits() || !request.Kind.Mutates() {
		return nil
	}

	estimate, err := s.engine.EstimatePrimitive(ctx, request)
	if err != nil {
		return fmt.Errorf("estimate engine primitive for admission: %w", err)
	}
	if !estimate.Applied {
		return nil
	}
	return s.admitCostGrowth(ctx, request.Key, estimate.OldCostBytes, estimate.NewCostBytes)
}

func (s *EngineService) admitCostGrowth(ctx context.Context, key Key, oldCost, nextCost uint64) error {
	if nextCost <= oldCost {
		return nil
	}
	delta := nextCost - oldCost

	stats, err := s.engine.Stats(ctx)
	if err != nil {
		return fmt.Errorf("read engine stats for admission: %w", err)
	}
	nsUsage, spaceUsage := usageFor(stats, key.Namespace, key.Space)

	nsUsage, _, err = s.ensureSpaceQuota(ctx, key, nsUsage, spaceUsage, delta)
	if err != nil {
		return err
	}
	return s.ensureNamespaceQuota(key, nsUsage, delta)
}

func (s *EngineService) ensureSpaceQuota(ctx context.Context, key Key, nsUsage, spaceUsage, delta uint64) (uint64, uint64, error) {
	limit := s.spaceLimit(key.Namespace, key.Space)
	if limit == 0 || spaceUsage+delta <= limit {
		return nsUsage, spaceUsage, nil
	}

	evicted, err := s.evictLocked(ctx, EvictRequest{
		Namespace:   key.Namespace,
		Space:       key.Space,
		TargetBytes: spaceUsage + delta - limit,
		Exclude:     key,
	})
	if err != nil {
		return nsUsage, spaceUsage, err
	}

	spaceUsage -= minUint64(spaceUsage, evicted.FreedBytes)
	nsUsage -= minUint64(nsUsage, evicted.FreedBytes)
	if spaceUsage+delta > limit {
		return nsUsage, spaceUsage, fmt.Errorf("%w: space %q/%q memory %d + %d > %d", ErrQuotaExceeded, key.Namespace, key.Space, spaceUsage, delta, limit)
	}
	return nsUsage, spaceUsage, nil
}

func (s *EngineService) ensureNamespaceQuota(key Key, nsUsage, delta uint64) error {
	if limit := s.namespaceLimit(key.Namespace); limit > 0 && nsUsage+delta > limit {
		return fmt.Errorf("%w: namespace %q memory %d + %d > %d", ErrQuotaExceeded, key.Namespace, nsUsage, delta, limit)
	}
	return nil
}

func (s *EngineService) namespaceLimit(namespace string) uint64 {
	if quota, ok := s.quota.Namespaces[namespace]; ok {
		return quota.MemoryBytes
	}
	return s.quota.DefaultNamespaceMemoryBytes
}

func (s *EngineService) spaceLimit(namespace, space string) uint64 {
	if quota, ok := s.quota.Spaces[SpaceRef{Namespace: namespace, Space: space}]; ok {
		return quota.MemoryBytes
	}
	return s.quota.DefaultSpaceMemoryBytes
}

func usageFor(stats Stats, namespace, space string) (namespaceMemoryBytes, spaceMemoryBytes uint64) {
	for _, item := range stats.Spaces {
		if item.Namespace != namespace {
			continue
		}
		namespaceMemoryBytes += item.MemoryBytes
		if item.Space == space {
			spaceMemoryBytes += item.MemoryBytes
		}
	}
	return namespaceMemoryBytes, spaceMemoryBytes
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func expectedVersionWillApply(found bool, version, expected uint64) bool {
	return expected == 0 || found && version == expected
}
