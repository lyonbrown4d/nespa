package engine

import (
	"context"
	"sort"

	"github.com/lyonbrown4d/nespa/routing"
)

func (e *MemoryEngine) Export(ctx context.Context, opts RangeOptions) (Snapshot, error) {
	if err := validateRangeOptions(opts); err != nil {
		return Snapshot{}, err
	}
	if opts.Now.IsZero() {
		opts.Now = e.now()
	}
	snapshot := Snapshot{}
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:      commandExport,
			rangeOpts: opts,
			reply:     make(chan shardResult, 1),
		})
		if err != nil {
			return Snapshot{}, err
		}
		if result.err != nil {
			return Snapshot{}, result.err
		}
		snapshot.Entries = append(snapshot.Entries, result.snapshot...)
	}
	sortSnapshotEntries(snapshot.Entries)
	return snapshot, nil
}

func (e *MemoryEngine) Import(ctx context.Context, snapshot Snapshot) (ImportResult, error) {
	entriesByShard := e.snapshotEntriesByShard(snapshot.Entries)
	total := ImportResult{}
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:     commandImport,
			snapshot: entriesByShard[worker],
			reply:    make(chan shardResult, 1),
		})
		if err != nil {
			return total, err
		}
		if result.err != nil {
			return total, result.err
		}
		total.Imported += result.imported
	}
	return total, nil
}

func (e *MemoryEngine) DeleteRange(ctx context.Context, opts RangeOptions) (DeleteRangeResult, error) {
	if err := validateRangeOptions(opts); err != nil {
		return DeleteRangeResult{}, err
	}
	total := DeleteRangeResult{}
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:      commandDeleteRange,
			rangeOpts: opts,
			reply:     make(chan shardResult, 1),
		})
		if err != nil {
			return total, err
		}
		if result.err != nil {
			return total, result.err
		}
		total.Deleted += result.deletedRange
	}
	return total, nil
}

func validateRangeOptions(opts RangeOptions) error {
	if opts.Namespace == "" || opts.Space == "" || opts.VSlotStart > opts.VSlotEnd {
		return ErrInvalidKey
	}
	return nil
}

func sortSnapshotEntries(entries []SnapshotEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return physicalKey(entries[i].Key) < physicalKey(entries[j].Key)
	})
}

func (s *shardWorker) exportEntries(opts RangeOptions) []SnapshotEntry {
	entries := make([]SnapshotEntry, 0)
	s.entries.Range(func(_ string, ent *entry) bool {
		if ent.expired(opts.Now) || !entryInRange(ent.key, opts) {
			return true
		}
		entries = append(entries, snapshotEntryFromEntry(ent))
		return true
	})
	return entries
}

func (s *shardWorker) importEntries(entries []SnapshotEntry) (uint64, error) {
	var imported uint64
	for index := range entries {
		if ok, err := s.importEntry(entries[index]); err != nil {
			return imported, err
		} else if ok {
			imported++
		}
	}
	return imported, nil
}

func (s *shardWorker) importEntry(item SnapshotEntry) (bool, error) {
	ent, err := entryFromSnapshot(item)
	if err != nil {
		return false, err
	}
	physical := physicalKey(ent.key)
	if existing, ok := s.entries.Get(physical); ok {
		return s.replaceImportedEntry(physical, existing, ent), nil
	}
	s.entries.Set(physical, ent)
	s.objects++
	s.memoryBytes += ent.costBytes
	s.addSpaceUsage(spaceKeyOf(ent.key), 1, ent.costBytes)
	return true, nil
}

func (s *shardWorker) replaceImportedEntry(physical string, existing, ent *entry) bool {
	if !shouldImportEntry(existing, ent) {
		return false
	}
	s.replaceEntryCost(existing.key, existing.costBytes, ent.costBytes)
	s.entries.Set(physical, ent)
	return true
}

func shouldImportEntry(existing, incoming *entry) bool {
	if incoming.version != existing.version {
		return incoming.version > existing.version
	}
	return incoming.updatedAt.After(existing.updatedAt)
}

func (s *shardWorker) deleteRange(opts RangeOptions) uint64 {
	targets := s.rangeDeleteTargets(opts)
	for index := range targets {
		s.deleteEntry(targets[index].physical, targets[index].entry)
	}
	return uint64(len(targets))
}

type rangeDeleteTarget struct {
	physical string
	entry    *entry
}

func (s *shardWorker) rangeDeleteTargets(opts RangeOptions) []rangeDeleteTarget {
	targets := make([]rangeDeleteTarget, 0)
	s.entries.Range(func(physical string, ent *entry) bool {
		if entryInRange(ent.key, opts) {
			targets = append(targets, rangeDeleteTarget{physical: physical, entry: ent})
		}
		return true
	})
	return targets
}

func entryInRange(key Key, opts RangeOptions) bool {
	if key.Namespace != opts.Namespace || key.Space != opts.Space {
		return false
	}
	slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
	return slot >= opts.VSlotStart && slot <= opts.VSlotEnd
}

func snapshotEntryFromEntry(ent *entry) SnapshotEntry {
	return SnapshotEntry{
		Key:              ent.key,
		Value:            append([]byte(nil), ent.value...),
		Version:          ent.version,
		NamespaceVersion: ent.namespaceVersion,
		SpaceVersion:     ent.spaceVersion,
		ExpireAt:         ent.expireAt,
		CreatedAt:        ent.createdAt,
		UpdatedAt:        ent.updatedAt,
		LastAccessAt:     ent.lastAccessAt,
		AccessCount:      ent.accessCount,
	}
}
