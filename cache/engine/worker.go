package engine

import (
	"time"

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
