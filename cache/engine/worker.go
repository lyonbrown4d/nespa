package engine

import (
	"math"
	"strconv"
	"strings"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/samber/oops"
)

func (s *shardWorker) run(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case cmd := <-s.commands:
			cmd.reply <- s.apply(cmd)
		}
	}
}

func (s *shardWorker) apply(cmd shardCommand) shardResult {
	switch cmd.kind {
	case commandSet:
		return s.applySet(cmd)
	case commandGet:
		return s.applyGet(cmd)
	case commandDelete:
		return s.applyDelete(cmd)
	case commandTouch:
		return s.applyTouch(cmd)
	case commandAdjust:
		return s.applyAdjust(cmd)
	case commandStats:
		return s.statsResult()
	case commandSweep:
		return shardResult{swept: s.sweepExpired(cmd.now)}
	case commandEvict:
		return shardResult{evicted: s.evict(cmd.evict)}
	default:
		return shardResult{err: oops.Code("unknown_shard_command").
			In("cache.engine").
			With("kind", cmd.kind).
			Errorf("engine: unknown shard command %d", cmd.kind)}
	}
}

func (s *shardWorker) statsResult() shardResult {
	return shardResult{stats: ShardStats{
		ID:            s.id,
		Objects:       s.objects,
		MemoryBytes:   s.memoryBytes,
		Evictions:     s.evictions,
		GetRequests:   s.gets,
		GetHits:       s.getHits,
		GetMisses:     s.getMisses,
		GetExpired:    s.getExpired,
		TouchRequests: s.touches,
		TouchHits:     s.touchHits,
		TouchMisses:   s.touchMisses,
		QueueDepth:    len(s.commands),
	}, spaces: s.spaces.Clone()}
}

func (s *shardWorker) applySet(cmd shardCommand) shardResult {
	if cmd.setOpts.ExpectedVersion > 0 {
		if existing, ok := s.entries.Get(cmd.physical); ok {
			if existing.version != cmd.setOpts.ExpectedVersion {
				return shardResult{found: false}
			}
		} else {
			return shardResult{found: false}
		}
	}

	var expireAt time.Time
	if cmd.setOpts.TTL > 0 {
		expireAt = cmd.now.Add(cmd.setOpts.TTL)
	}

	cost := costOf(cmd.key, cmd.value)
	if existing, ok := s.entries.Get(cmd.physical); ok {
		s.replaceEntry(existing, cmd, expireAt, cost)
		return shardResult{record: existing.record(), found: true}
	}

	ent := newEntry(cmd, expireAt, cost)
	s.entries.Set(cmd.physical, ent)
	s.objects++
	s.memoryBytes += cost
	s.addSpaceUsage(spaceKeyOf(cmd.key), 1, cost)
	return shardResult{record: ent.record(), found: true}
}

func (s *shardWorker) replaceEntry(existing *entry, cmd shardCommand, expireAt time.Time, cost uint64) {
	if cost >= existing.costBytes {
		delta := cost - existing.costBytes
		s.memoryBytes += delta
		s.addSpaceUsage(spaceKeyOf(existing.key), 0, delta)
	} else {
		delta := existing.costBytes - cost
		s.memoryBytes -= delta
		s.subtractSpaceUsage(spaceKeyOf(existing.key), 0, delta)
	}
	existing.value = cmd.value
	existing.version++
	existing.namespaceVersion = cmd.setOpts.NamespaceVersion
	existing.spaceVersion = cmd.setOpts.SpaceVersion
	existing.expireAt = expireAt
	existing.updatedAt = cmd.now
	existing.lastAccessAt = cmd.now
	existing.accessCount++
	existing.costBytes = cost
}

func (s *shardWorker) applyGet(cmd shardCommand) shardResult {
	s.gets++
	ent, ok := s.entries.Get(cmd.physical)
	if !ok {
		s.getMisses++
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		s.getExpired++
		s.getMisses++
		return shardResult{}
	}
	if !ent.visible(cmd.getOpts) {
		s.getMisses++
		return shardResult{}
	}
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	s.getHits++
	return shardResult{record: ent.record(), found: true}
}

func (s *shardWorker) applyDelete(cmd shardCommand) shardResult {
	ent, ok := s.entries.Get(cmd.physical)
	if !ok {
		return shardResult{}
	}
	if cmd.deleteOpts.ExpectedVersion > 0 && ent.version != cmd.deleteOpts.ExpectedVersion {
		return shardResult{found: false}
	}
	s.deleteEntry(cmd.physical, ent)
	return shardResult{deleted: true, found: true}
}

func (s *shardWorker) applyTouch(cmd shardCommand) shardResult {
	s.touches++
	ent, ok := s.entries.Get(cmd.physical)
	if !ok {
		s.touchMisses++
		return shardResult{}
	}
	if cmd.touch.TTL < 0 {
		s.touchMisses++
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		s.touchMisses++
		return shardResult{}
	}
	if cmd.touch.ExpectedVersion > 0 && ent.version != cmd.touch.ExpectedVersion {
		s.touchMisses++
		return shardResult{}
	}
	if !ent.visible(GetOptions{NamespaceVersion: cmd.touch.NamespaceVersion, SpaceVersion: cmd.touch.SpaceVersion}) {
		s.touchMisses++
		return shardResult{}
	}

	if cmd.touch.TTL > 0 {
		ent.expireAt = cmd.now.Add(cmd.touch.TTL)
	} else {
		ent.expireAt = time.Time{}
	}
	ent.updatedAt = cmd.now
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	s.touchHits++
	return shardResult{touched: true}
}

func (s *shardWorker) applyAdjust(cmd shardCommand) shardResult {
	existing, exists := s.entries.Get(cmd.physical)
	if exists && cmd.adjust.ExpectedVersion > 0 && existing.version != cmd.adjust.ExpectedVersion {
		return shardResult{found: false}
	}

	var nextValue int64
	var expireAt time.Time
	var err error

	switch {
	case !exists:
		if cmd.adjust.ExpectedVersion > 0 {
			return shardResult{found: false}
		}
		nextValue, err = safeAdd(cmd.adjust.InitialValue, cmd.adjust.Delta)
		if err != nil {
			return shardResult{err: err}
		}
		if cmd.adjust.TTL > 0 {
			expireAt = cmd.now.Add(cmd.adjust.TTL)
		}
	case existing.expired(cmd.now):
		s.deleteEntry(cmd.physical, existing)
		return shardResult{found: false}
	default:
		nextValue, err = addToCurrentCounter(existing.value, cmd.adjust.Delta)
		if err != nil {
			return shardResult{err: err}
		}
		expireAt = existing.expireAt
	}

	if err != nil {
		return shardResult{err: err}
	}

	value := []byte(strconv.FormatInt(nextValue, 10))
	cost := costOf(cmd.key, value)

	if exists {
		oldCost := existing.costBytes
		existing.value = value
		existing.version++
		existing.namespaceVersion = cmd.adjust.NamespaceVersion
		existing.spaceVersion = cmd.adjust.SpaceVersion
		existing.expireAt = expireAt
		existing.updatedAt = cmd.now
		existing.lastAccessAt = cmd.now
		existing.accessCount++
		existing.costBytes = cost

		if cost >= oldCost {
			delta := cost - oldCost
			s.memoryBytes += delta
			s.addSpaceUsage(spaceKeyOf(cmd.key), 0, delta)
		} else {
			delta := oldCost - cost
			s.memoryBytes -= delta
			s.subtractSpaceUsage(spaceKeyOf(cmd.key), 0, delta)
		}
		return shardResult{record: existing.record(), found: true}
	}

	ent := &entry{
		key:              cmd.key,
		value:            append([]byte(nil), value...),
		version:          1,
		namespaceVersion: cmd.adjust.NamespaceVersion,
		spaceVersion:     cmd.adjust.SpaceVersion,
		expireAt:         expireAt,
		createdAt:        cmd.now,
		updatedAt:        cmd.now,
		lastAccessAt:     cmd.now,
		accessCount:      1,
		costBytes:        cost,
	}
	s.entries.Set(cmd.physical, ent)
	s.objects++
	s.memoryBytes += cost
	s.addSpaceUsage(spaceKeyOf(cmd.key), 1, cost)
	return shardResult{record: ent.record(), found: true}
}

func addToCurrentCounter(raw []byte, delta int64) (int64, error) {
	text := strings.TrimSpace(string(raw))
	current, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, oops.Code("invalid_counter_value").
			In("cache.engine").
			With("value", text, "parse_error", err.Error()).
			Wrap(ErrInvalidCounter)
	}
	return safeAdd(current, delta)
}

func safeAdd(base, delta int64) (int64, error) {
	if delta > 0 && base > math.MaxInt64-delta {
		return 0, oops.Code("counter_overflow").
			In("cache.engine").
			With("base", base, "delta", delta).
			Wrap(ErrInvalidCounter)
	}
	if delta < 0 && base < math.MinInt64-delta {
		return 0, oops.Code("counter_overflow").
			In("cache.engine").
			With("base", base, "delta", delta).
			Wrap(ErrInvalidCounter)
	}
	return base + delta, nil
}

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
	expired := collectionlist.NewList[struct {
		physical string
		ent      *entry
	}]()
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
			expired.Add(struct {
				physical string
				ent      *entry
			}{physical: physical, ent: ent})
			return true
		}
		candidates.Add(ent)
		return true
	})
	expired.Range(func(_ int, item struct {
		physical string
		ent      *entry
	}) bool {
		s.deleteEntry(item.physical, item.ent)
		return true
	})
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
	expired := collectionlist.NewList[struct {
		physical string
		ent      *entry
	}]()
	s.entries.Range(func(physical string, ent *entry) bool {
		if ent.expired(now) {
			expired.Add(struct {
				physical string
				ent      *entry
			}{physical: physical, ent: ent})
		}
		return true
	})
	expired.Range(func(_ int, item struct {
		physical string
		ent      *entry
	}) bool {
		s.deleteEntry(item.physical, item.ent)
		return true
	})
	return checkedUint64(expired.Len())
}

func checkedUint64(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func (s *shardWorker) deleteEntry(physical string, ent *entry) {
	s.entries.Delete(physical)
	s.objects--
	s.memoryBytes -= ent.costBytes
	s.subtractSpaceUsage(spaceKeyOf(ent.key), 1, ent.costBytes)
}

func (s *shardWorker) addSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage, _ := s.spaces.Get(key)
	usage.objects += objects
	usage.memoryBytes += memoryBytes
	s.spaces.Set(key, usage)
}

func (s *shardWorker) subtractSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage, _ := s.spaces.Get(key)
	usage.objects -= objects
	usage.memoryBytes -= memoryBytes
	if usage.objects == 0 && usage.memoryBytes == 0 {
		s.spaces.Delete(key)
		return
	}
	s.spaces.Set(key, usage)
}
