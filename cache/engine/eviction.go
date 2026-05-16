package engine

import (
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *shardWorker) evict(opts EvictOptions) EvictResult {
	result := EvictResult{RequestedBytes: opts.TargetBytes}
	candidates := s.collectEvictionCandidates(opts, &result)
	candidates.Sort(func(left, right *entry) int {
		if left.lastAccessAt.Equal(right.lastAccessAt) {
			return compareTime(left.createdAt, right.createdAt)
		}
		return compareTime(left.lastAccessAt, right.lastAccessAt)
	})
	s.evictCandidates(candidates, opts.TargetBytes, &result)
	return result
}

func (s *shardWorker) collectEvictionCandidates(opts EvictOptions, result *EvictResult) *collectionlist.List[*entry] {
	candidates := collectionlist.NewList[*entry]()
	expired := collectionlist.NewList[expiredEntry]()
	excludePhysical := ""
	if opts.ExcludeActive {
		excludePhysical = physicalKey(opts.Exclude)
	}

	s.entries.Range(func(physical string, ent *entry) bool {
		if !evictionCandidate(ent, opts, physical, excludePhysical) {
			return true
		}
		if ent.expired(opts.Now) {
			result.FreedBytes += ent.costBytes
			result.EvictedObjects++
			expired.Add(expiredEntry{physical: physical, ent: ent})
			return true
		}
		candidates.Add(ent)
		return true
	})
	s.deleteExpired(expired)
	return candidates
}

func compareTime(left, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case right.Before(left):
		return 1
	default:
		return 0
	}
}

func (s *shardWorker) evictCandidates(candidates *collectionlist.List[*entry], target uint64, result *EvictResult) {
	candidates.Range(func(_ int, ent *entry) bool {
		if result.FreedBytes >= target {
			return false
		}
		result.FreedBytes += ent.costBytes
		result.EvictedObjects++
		s.deleteEntry(physicalKey(ent.key), ent)
		return true
	})
	if result.EvictedObjects > 0 {
		s.evictions += result.EvictedObjects
	}
}

func evictionCandidate(ent *entry, opts EvictOptions, physical, excludePhysical string) bool {
	if excludePhysical != "" && physical == excludePhysical {
		return false
	}
	return ent.key.Namespace == opts.Namespace && ent.key.Space == opts.Space
}

func (s *shardWorker) sweepExpired(now time.Time) uint64 {
	expired := collectionlist.NewList[expiredEntry]()
	s.entries.Range(func(physical string, ent *entry) bool {
		if ent.expired(now) {
			expired.Add(expiredEntry{physical: physical, ent: ent})
		}
		return true
	})
	s.deleteExpired(expired)
	return checkedUint64(expired.Len())
}

type expiredEntry struct {
	physical string
	ent      *entry
}

func (s *shardWorker) deleteExpired(expired *collectionlist.List[expiredEntry]) {
	expired.Range(func(_ int, item expiredEntry) bool {
		s.deleteEntry(item.physical, item.ent)
		return true
	})
}

func checkedUint64(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
