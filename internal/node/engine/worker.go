package engine

import (
	"fmt"
	"maps"
	"sort"
	"time"
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
	case commandStats:
		return s.statsResult()
	case commandSweep:
		return shardResult{swept: s.sweepExpired(cmd.now)}
	case commandEvict:
		return shardResult{evicted: s.evict(cmd.evict)}
	default:
		return shardResult{err: fmt.Errorf("engine: unknown shard command %d", cmd.kind)}
	}
}

func (s *shardWorker) statsResult() shardResult {
	spaces := make(map[spaceKey]spaceUsage, len(s.spaces))
	maps.Copy(spaces, s.spaces)
	return shardResult{stats: ShardStats{
		ID:          s.id,
		Objects:     s.objects,
		MemoryBytes: s.memoryBytes,
		Evictions:   s.evictions,
		QueueDepth:  len(s.commands),
	}, spaces: spaces}
}

func (s *shardWorker) applySet(cmd shardCommand) shardResult {
	var expireAt time.Time
	if cmd.setOpts.TTL > 0 {
		expireAt = cmd.now.Add(cmd.setOpts.TTL)
	}

	cost := costOf(cmd.key, cmd.value)
	if existing, ok := s.entries[cmd.physical]; ok {
		s.replaceEntry(existing, cmd, expireAt, cost)
		return shardResult{record: existing.record()}
	}

	ent := newEntry(cmd, expireAt, cost)
	s.entries[cmd.physical] = ent
	s.objects++
	s.memoryBytes += cost
	s.addSpaceUsage(spaceKeyOf(cmd.key), 1, cost)
	return shardResult{record: ent.record()}
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
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return shardResult{}
	}
	if !ent.visible(cmd.getOpts) {
		return shardResult{}
	}
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	return shardResult{record: ent.record(), found: true}
}

func (s *shardWorker) applyDelete(cmd shardCommand) shardResult {
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	s.deleteEntry(cmd.physical, ent)
	return shardResult{deleted: true}
}

func (s *shardWorker) applyTouch(cmd shardCommand) shardResult {
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return shardResult{}
	}

	ent.expireAt = cmd.now.Add(cmd.touch.TTL)
	ent.updatedAt = cmd.now
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	return shardResult{touched: true}
}

func (s *shardWorker) evict(opts EvictOptions) EvictResult {
	result := EvictResult{RequestedBytes: opts.TargetBytes}
	candidates := s.collectEvictionCandidates(opts, &result)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lastAccessAt.Equal(candidates[j].lastAccessAt) {
			return candidates[i].createdAt.Before(candidates[j].createdAt)
		}
		return candidates[i].lastAccessAt.Before(candidates[j].lastAccessAt)
	})
	s.evictCandidates(candidates, opts.TargetBytes, &result)
	return result
}

func (s *shardWorker) collectEvictionCandidates(opts EvictOptions, result *EvictResult) []*entry {
	candidates := make([]*entry, 0)
	excludePhysical := ""
	if opts.ExcludeActive {
		excludePhysical = physicalKey(opts.Exclude)
	}

	for physical, ent := range s.entries {
		if !evictionCandidate(ent, opts, physical, excludePhysical) {
			continue
		}
		if ent.expired(opts.Now) {
			result.FreedBytes += ent.costBytes
			result.EvictedObjects++
			s.deleteEntry(physical, ent)
			continue
		}
		candidates = append(candidates, ent)
	}
	return candidates
}

func (s *shardWorker) evictCandidates(candidates []*entry, target uint64, result *EvictResult) {
	for _, ent := range candidates {
		if result.FreedBytes >= target {
			break
		}
		result.FreedBytes += ent.costBytes
		result.EvictedObjects++
		s.deleteEntry(physicalKey(ent.key), ent)
	}
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
	var deleted uint64
	for physical, ent := range s.entries {
		if ent.expired(now) {
			s.deleteEntry(physical, ent)
			deleted++
		}
	}
	return deleted
}

func (s *shardWorker) deleteEntry(physical string, ent *entry) {
	delete(s.entries, physical)
	s.objects--
	s.memoryBytes -= ent.costBytes
	s.subtractSpaceUsage(spaceKeyOf(ent.key), 1, ent.costBytes)
}

func (s *shardWorker) addSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage := s.spaces[key]
	usage.objects += objects
	usage.memoryBytes += memoryBytes
	s.spaces[key] = usage
}

func (s *shardWorker) subtractSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage := s.spaces[key]
	usage.objects -= objects
	usage.memoryBytes -= memoryBytes
	if usage.objects == 0 && usage.memoryBytes == 0 {
		delete(s.spaces, key)
		return
	}
	s.spaces[key] = usage
}
