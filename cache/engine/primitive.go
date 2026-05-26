package engine

import (
	"bytes"
	"time"
)

type primitiveHandler func(*shardWorker, shardCommand) shardResult

var primitiveHandlers = map[PrimitiveKind]primitiveHandler{
	PrimitiveCounterAdjust:   (*shardWorker).applyCounterPrimitive,
	PrimitiveMapSet:          (*shardWorker).applyMapSet,
	PrimitiveMapGet:          (*shardWorker).applyMapGet,
	PrimitiveMapDelete:       (*shardWorker).applyMapDelete,
	PrimitiveMapGetAll:       (*shardWorker).applyMapGetAll,
	PrimitiveSetAdd:          (*shardWorker).applySetAdd,
	PrimitiveSetRemove:       (*shardWorker).applySetRemove,
	PrimitiveSetContains:     (*shardWorker).applySetContains,
	PrimitiveSetMembers:      (*shardWorker).applySetMembers,
	PrimitiveScoredSetPut:    (*shardWorker).applyScoredSetPut,
	PrimitiveScoredSetRemove: (*shardWorker).applyScoredSetRemove,
	PrimitiveScoredSetRange:  (*shardWorker).applyScoredSetRange,
	PrimitiveListPushFront:   (*shardWorker).applyListPushFront,
	PrimitiveListPushBack:    (*shardWorker).applyListPushBack,
	PrimitiveListPopFront:    (*shardWorker).applyListPopFront,
	PrimitiveListPopBack:     (*shardWorker).applyListPopBack,
	PrimitiveListRange:       (*shardWorker).applyListRange,
	PrimitiveBitmapSetBit:    (*shardWorker).applyBitmapSetBit,
	PrimitiveBitmapGetBit:    (*shardWorker).applyBitmapGetBit,
	PrimitiveBitmapBitCount:  (*shardWorker).applyBitmapBitCount,
	PrimitiveHLLAdd:          (*shardWorker).applyHLLAdd,
	PrimitiveHLLCount:        (*shardWorker).applyHLLCount,
	PrimitiveHLLMerge:        (*shardWorker).applyHLLMerge,
	PrimitiveHLLMembers:      (*shardWorker).applyHLLMembers,
	PrimitiveGeoAdd:          (*shardWorker).applyGeoAdd,
	PrimitiveGeoDist:         (*shardWorker).applyGeoDist,
	PrimitiveGeoRadius:       (*shardWorker).applyGeoRadius,
}

func (s *shardWorker) applyPrimitive(cmd shardCommand) shardResult {
	handler, ok := primitiveHandlers[cmd.primitive.Kind]
	if !ok {
		return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown kind")}
	}
	return handler(s, cmd)
}

func (s *shardWorker) applyCounterPrimitive(cmd shardCommand) shardResult {
	next := cmd
	next.kind = commandAdjust
	next.adjust = AdjustOptions{
		Delta:            cmd.primitive.Delta,
		InitialValue:     cmd.primitive.InitialValue,
		TTL:              cmd.primitive.Options.TTL,
		NamespaceVersion: cmd.primitive.Options.NamespaceVersion,
		SpaceVersion:     cmd.primitive.Options.SpaceVersion,
		ExpectedVersion:  cmd.primitive.Options.ExpectedVersion,
	}
	result := s.applyAdjust(next)
	if result.err != nil || !result.found {
		return shardResult{err: result.err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record:  result.record,
		Found:   true,
		Applied: true,
		Value:   result.record.Value,
	}}
}

func (s *shardWorker) readPrimitiveEntry(cmd shardCommand) (*entry, bool) {
	ent, ok := s.entries.Get(cmd.physical)
	if !ok {
		return nil, false
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return nil, false
	}
	if !ent.visible(primitiveGetOptions(cmd.primitive.Options)) {
		return nil, false
	}
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	return ent, true
}

func (s *shardWorker) writePrimitiveEntry(cmd shardCommand, ent *entry, value []byte, count uint64) PrimitiveResult {
	if bytes.Equal(ent.value, value) {
		return primitiveWriteResult(ent, count)
	}
	expireAt := primitiveExpireAt(cmd.primitive.Options, cmd.now, ent.expireAt)
	s.replacePrimitiveEntry(ent, cmd, value, expireAt)
	return primitiveWriteResult(ent, count)
}

func (s *shardWorker) createPrimitiveEntry(cmd shardCommand, value []byte, count uint64) PrimitiveResult {
	expireAt := primitiveExpireAt(cmd.primitive.Options, cmd.now, time.Time{})
	cost := costOf(cmd.key, value)
	ent := &entry{
		key:              cmd.key,
		value:            append([]byte(nil), value...),
		version:          1,
		namespaceVersion: cmd.primitive.Options.NamespaceVersion,
		spaceVersion:     cmd.primitive.Options.SpaceVersion,
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
	return primitiveWriteResult(ent, count)
}

func (s *shardWorker) replacePrimitiveEntry(ent *entry, cmd shardCommand, value []byte, expireAt time.Time) {
	cost := costOf(cmd.key, value)
	s.replaceEntryCost(ent.key, ent.costBytes, cost)
	ent.value = append([]byte(nil), value...)
	ent.version++
	ent.namespaceVersion = cmd.primitive.Options.NamespaceVersion
	ent.spaceVersion = cmd.primitive.Options.SpaceVersion
	ent.expireAt = expireAt
	ent.updatedAt = cmd.now
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	ent.costBytes = cost
}

func (s *shardWorker) mutablePrimitiveEntry(cmd shardCommand) (*entry, bool, bool) {
	ent, exists := s.entries.Get(cmd.physical)
	if exists && ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return nil, false, true
	}
	if !primitiveExpectedVersionMatches(ent, exists, cmd.primitive.Options.ExpectedVersion) {
		return nil, exists, false
	}
	return ent, exists, true
}

func primitiveExpectedVersionMatches(ent *entry, exists bool, expected uint64) bool {
	if expected == 0 {
		return true
	}
	return exists && ent.version == expected
}

func primitiveExpireAt(opts PrimitiveOptions, now, existing time.Time) time.Time {
	if opts.TTL > 0 {
		return now.Add(opts.TTL)
	}
	return existing
}

func primitiveGetOptions(opts PrimitiveOptions) GetOptions {
	return GetOptions{NamespaceVersion: opts.NamespaceVersion, SpaceVersion: opts.SpaceVersion}
}

func primitiveWriteResult(ent *entry, count uint64) PrimitiveResult {
	return PrimitiveResult{Record: ent.record(), Found: true, Applied: true, Count: count}
}

func primitiveMiss() shardResult {
	return shardResult{primitive: PrimitiveResult{}}
}
