package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/routing"
)

func TestMemoryEngineRangeMigration(t *testing.T) {
	now := time.UnixMilli(1000)
	source := engine.NewMemory(engine.Config{ShardCount: 4, Now: func() time.Time { return now }})
	defer closeEngine(t, source)
	target := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, target)

	moving := engine.Key{Namespace: "orders", Space: "session", Key: "moving"}
	slot := routing.VSlotFor(moving.Namespace, moving.Space, moving.Key)
	staying := keyOutsideSlot(t, moving, slot)

	seed, _, err := source.Set(t.Context(), moving, []byte("initial"), engine.SetOptions{})
	if err != nil {
		t.Fatalf("seed moving key: %v", err)
	}
	now = now.Add(time.Millisecond)
	updated, _, err := source.Set(t.Context(), moving, []byte("migrated"), engine.SetOptions{
		ExpectedVersion: seed.Version,
	})
	if err != nil {
		t.Fatalf("update moving key: %v", err)
	}
	if _, _, stayErr := source.Set(t.Context(), staying, []byte("stay"), engine.SetOptions{}); stayErr != nil {
		t.Fatalf("seed staying key: %v", stayErr)
	}

	snapshot, err := source.Export(t.Context(), engine.RangeOptions{
		Namespace:  moving.Namespace,
		Space:      moving.Space,
		VSlotStart: slot,
		VSlotEnd:   slot,
	})
	if err != nil {
		t.Fatalf("export range: %v", err)
	}
	requireSnapshotSize(t, snapshot, 1)

	result, err := target.Import(t.Context(), snapshot)
	if err != nil {
		t.Fatalf("import snapshot: %v", err)
	}
	if result.Imported != 1 {
		t.Fatalf("imported = %d, want 1", result.Imported)
	}
	requireEngineVersionedValue(t, target, moving, "migrated", updated.Version)

	deleted, err := source.DeleteRange(t.Context(), engine.RangeOptions{
		Namespace:  moving.Namespace,
		Space:      moving.Space,
		VSlotStart: slot,
		VSlotEnd:   slot,
	})
	if err != nil {
		t.Fatalf("delete range: %v", err)
	}
	if deleted.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted.Deleted)
	}
	requireEngineMissing(t, source, moving, "moved key")
	requireEngineFound(t, source, staying, "staying key")
}

func TestMemoryEngineImportSkipsOlderVersion(t *testing.T) {
	source := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, source)
	target := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, target)

	key := engine.Key{Namespace: "orders", Space: "session", Key: "counter"}
	if _, _, err := source.Set(t.Context(), key, []byte("old"), engine.SetOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, _, err := target.Set(t.Context(), key, []byte("new-1"), engine.SetOptions{}); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	if _, _, err := target.Set(t.Context(), key, []byte("new-2"), engine.SetOptions{}); err != nil {
		t.Fatalf("update target: %v", err)
	}

	slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
	snapshot, err := source.Export(t.Context(), engine.RangeOptions{
		Namespace:  key.Namespace,
		Space:      key.Space,
		VSlotStart: slot,
		VSlotEnd:   slot,
	})
	if err != nil {
		t.Fatalf("export source: %v", err)
	}
	result, err := target.Import(t.Context(), snapshot)
	if err != nil {
		t.Fatalf("import older snapshot: %v", err)
	}
	if result.Imported != 0 {
		t.Fatalf("imported = %d, want 0", result.Imported)
	}
	requireEngineVersionedValue(t, target, key, "new-2", 2)
}

func keyOutsideSlot(t *testing.T, base engine.Key, slot uint32) engine.Key {
	t.Helper()
	for index := range 100_000 {
		candidate := base
		candidate.Key = fmt.Sprintf("staying-%d", index)
		if routing.VSlotFor(candidate.Namespace, candidate.Space, candidate.Key) != slot {
			return candidate
		}
	}
	t.Fatal("failed to find key outside slot")
	return engine.Key{}
}

func requireSnapshotSize(t *testing.T, snapshot engine.Snapshot, want int) {
	t.Helper()
	if len(snapshot.Entries) != want {
		t.Fatalf("snapshot entries = %d, want %d", len(snapshot.Entries), want)
	}
}

func requireEngineVersionedValue(t *testing.T, eng engine.Engine, key engine.Key, want string, version uint64) {
	t.Helper()
	record, ok, err := eng.Get(t.Context(), key, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get record: ok=%v err=%v", ok, err)
	}
	if string(record.Value) != want || record.Version != version {
		t.Fatalf("record = value %q version %d, want %q version %d", record.Value, record.Version, want, version)
	}
}
