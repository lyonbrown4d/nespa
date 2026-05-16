package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (e *MemoryEngine) Snapshot(ctx context.Context) (Snapshot, error) {
	snapshot := Snapshot{}
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:  commandSnapshot,
			reply: make(chan shardResult, 1),
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

func (e *MemoryEngine) Restore(ctx context.Context, snapshot Snapshot) error {
	entriesByShard := e.snapshotEntriesByShard(snapshot.Entries)
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:     commandRestore,
			snapshot: entriesByShard[worker],
			reply:    make(chan shardResult, 1),
		})
		if err != nil {
			return err
		}
		if result.err != nil {
			return result.err
		}
	}
	return nil
}

func LoadSnapshotFile(path string) (Snapshot, error) {
	dir, name := snapshotDirAndName(path)
	raw, err := fs.ReadFile(os.DirFS(dir), name)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read engine snapshot: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode engine snapshot: %w", err)
	}
	return snapshot, nil
}

func SaveSnapshotFile(path string, snapshot Snapshot) error {
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode engine snapshot: %w", err)
	}
	dir, name := snapshotDirAndName(path)
	if mkdirErr := os.MkdirAll(dir, 0o750); mkdirErr != nil {
		return fmt.Errorf("create engine snapshot dir: %w", mkdirErr)
	}
	tmp, err := os.CreateTemp(dir, "."+name+".*.tmp")
	if err != nil {
		return fmt.Errorf("create engine snapshot temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, writeErr := tmp.Write(raw); writeErr != nil {
		return errors.Join(
			fmt.Errorf("write engine snapshot temp file: %w", writeErr),
			closeSnapshotTemp(tmp),
			removeSnapshotTemp(tmpName),
		)
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(
			fmt.Errorf("close engine snapshot temp file: %w", err),
			removeSnapshotTemp(tmpName),
		)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return errors.Join(
			fmt.Errorf("write engine snapshot: %w", err),
			removeSnapshotTemp(tmpName),
		)
	}
	return nil
}

func closeSnapshotTemp(file *os.File) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("close engine snapshot temp file: %w", err)
	}
	return nil
}

func removeSnapshotTemp(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove engine snapshot temp file: %w", err)
	}
	return nil
}

func snapshotDirAndName(path string) (string, string) {
	clean := filepath.Clean(path)
	dir, name := filepath.Split(clean)
	if dir == "" {
		dir = "."
	}
	return dir, name
}

func (e *MemoryEngine) snapshotEntriesByShard(entries []SnapshotEntry) map[*shardWorker][]SnapshotEntry {
	out := make(map[*shardWorker][]SnapshotEntry, len(e.shards))
	for index := range entries {
		physical := physicalKey(entries[index].Key)
		worker := e.shardFor(physical)
		out[worker] = append(out[worker], cloneSnapshotEntry(entries[index]))
	}
	return out
}

func (s *shardWorker) snapshotEntries() []SnapshotEntry {
	entries := make([]SnapshotEntry, 0, s.entries.Len())
	s.entries.Range(func(_ string, ent *entry) bool {
		entries = append(entries, snapshotEntryFromEntry(ent))
		return true
	})
	return entries
}

func (s *shardWorker) restoreEntries(entries []SnapshotEntry) error {
	s.entries = collectionmapping.NewMap[string, *entry]()
	s.spaces = collectionmapping.NewMap[spaceKey, spaceUsage]()
	s.objects = 0
	s.memoryBytes = 0
	s.evictions = 0
	s.gets = 0
	s.getHits = 0
	s.getMisses = 0
	s.getExpired = 0
	s.touches = 0
	s.touchHits = 0
	s.touchMisses = 0

	for index := range entries {
		ent, err := entryFromSnapshot(entries[index])
		if err != nil {
			return err
		}
		physical := physicalKey(ent.key)
		s.entries.Set(physical, ent)
		s.objects++
		s.memoryBytes += ent.costBytes
		s.addSpaceUsage(spaceKeyOf(ent.key), 1, ent.costBytes)
	}
	return nil
}

func cloneSnapshotEntry(item SnapshotEntry) SnapshotEntry {
	item.Value = append([]byte(nil), item.Value...)
	return item
}
