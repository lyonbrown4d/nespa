package cache

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/internal/node/engine"
)

func (s *EngineService) hasQuotaLimits() bool {
	return s.quota.DefaultNamespaceMemoryBytes > 0 || s.quota.DefaultSpaceMemoryBytes > 0 ||
		len(s.quota.Namespaces) > 0 || len(s.quota.Spaces) > 0
}

func (s *EngineService) currentCost(ctx context.Context, key Key) (uint64, error) {
	current, found, err := s.engine.Get(ctx, key, engine.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get engine record for admission: %w", err)
	}
	if !found {
		return 0, nil
	}
	return current.CostBytes, nil
}

func (s *EngineService) ensureSpaceQuota(ctx context.Context, key Key, nsUsage, spaceUsage, delta uint64) (uint64, uint64, error) {
	limit := s.spaceLimit(key.Namespace, key.Space)
	if limit == 0 || spaceUsage+delta <= limit {
		return nsUsage, spaceUsage, nil
	}

	evicted, err := s.Evict(ctx, EvictRequest{
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
