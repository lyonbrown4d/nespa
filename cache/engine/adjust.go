package engine

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/samber/oops"
)

type counterAdjustment struct {
	existing *entry
	exists   bool
	next     int64
	expireAt time.Time
}

func (s *shardWorker) applyAdjust(cmd shardCommand) shardResult {
	adjustment, ok, err := s.prepareCounterAdjustment(cmd)
	switch {
	case err != nil:
		return shardResult{err: err}
	case !ok:
		return shardResult{found: false}
	case adjustment.exists:
		return s.applyExistingCounter(cmd, adjustment)
	default:
		return s.applyNewCounter(cmd, adjustment)
	}
}

func (s *shardWorker) prepareCounterAdjustment(cmd shardCommand) (counterAdjustment, bool, error) {
	existing, exists := s.entries.Get(cmd.physical)
	if exists && cmd.adjust.ExpectedVersion > 0 && existing.version != cmd.adjust.ExpectedVersion {
		return counterAdjustment{}, false, nil
	}
	if !exists {
		return newCounterAdjustment(cmd)
	}
	if existing.expired(cmd.now) {
		s.deleteEntry(cmd.physical, existing)
		return counterAdjustment{}, false, nil
	}
	next, err := addToCurrentCounter(existing.value, cmd.adjust.Delta)
	if err != nil {
		return counterAdjustment{}, false, err
	}
	return counterAdjustment{
		existing: existing,
		exists:   true,
		next:     next,
		expireAt: existing.expireAt,
	}, true, nil
}

func newCounterAdjustment(cmd shardCommand) (counterAdjustment, bool, error) {
	if cmd.adjust.ExpectedVersion > 0 {
		return counterAdjustment{}, false, nil
	}
	next, err := safeAdd(cmd.adjust.InitialValue, cmd.adjust.Delta)
	if err != nil {
		return counterAdjustment{}, false, err
	}
	var expireAt time.Time
	if cmd.adjust.TTL > 0 {
		expireAt = cmd.now.Add(cmd.adjust.TTL)
	}
	return counterAdjustment{next: next, expireAt: expireAt}, true, nil
}

func (s *shardWorker) applyExistingCounter(cmd shardCommand, adjustment counterAdjustment) shardResult {
	existing := adjustment.existing
	value := []byte(strconv.FormatInt(adjustment.next, 10))
	cost := costOf(cmd.key, value)
	oldCost := existing.costBytes

	existing.value = value
	existing.version++
	existing.namespaceVersion = cmd.adjust.NamespaceVersion
	existing.spaceVersion = cmd.adjust.SpaceVersion
	existing.expireAt = adjustment.expireAt
	existing.updatedAt = cmd.now
	existing.lastAccessAt = cmd.now
	existing.accessCount++
	existing.costBytes = cost

	s.replaceCounterCost(cmd.key, oldCost, cost)
	return shardResult{record: existing.record(), found: true}
}

func (s *shardWorker) replaceCounterCost(key Key, oldCost, cost uint64) {
	if cost >= oldCost {
		delta := cost - oldCost
		s.memoryBytes += delta
		s.addSpaceUsage(spaceKeyOf(key), 0, delta)
		return
	}
	delta := oldCost - cost
	s.memoryBytes -= delta
	s.subtractSpaceUsage(spaceKeyOf(key), 0, delta)
}

func (s *shardWorker) applyNewCounter(cmd shardCommand, adjustment counterAdjustment) shardResult {
	value := []byte(strconv.FormatInt(adjustment.next, 10))
	cost := costOf(cmd.key, value)
	ent := &entry{
		key:              cmd.key,
		value:            append([]byte(nil), value...),
		version:          1,
		namespaceVersion: cmd.adjust.NamespaceVersion,
		spaceVersion:     cmd.adjust.SpaceVersion,
		expireAt:         adjustment.expireAt,
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
		return 0, counterOverflowError(base, delta)
	}
	if delta < 0 && base < math.MinInt64-delta {
		return 0, counterOverflowError(base, delta)
	}
	return base + delta, nil
}

func counterOverflowError(base, delta int64) error {
	return oops.Code("counter_overflow").
		In("cache.engine").
		With("base", base, "delta", delta).
		Wrap(ErrInvalidCounter)
}
