package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineSnapshotRestore(t *testing.T) {
	now := time.Unix(100, 0)
	source := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, source)

	key := engine.Key{Namespace: "orders", Space: "session", Key: "snapshot"}
	record, found, err := source.Set(context.Background(), key, []byte("value"), engine.SetOptions{
		TTL:              time.Minute,
		NamespaceVersion: 2,
		SpaceVersion:     3,
	})
	if err != nil {
		t.Fatalf("set source record: %v", err)
	}
	if !found {
		t.Fatal("source set should return found")
	}

	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot source: %v", err)
	}

	target := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, target)
	if restoreErr := target.Restore(context.Background(), snapshot); restoreErr != nil {
		t.Fatalf("restore snapshot: %v", restoreErr)
	}

	restored, ok, err := target.Get(context.Background(), key, engine.GetOptions{
		NamespaceVersion: 2,
		SpaceVersion:     3,
	})
	if err != nil {
		t.Fatalf("get restored record: %v", err)
	}
	if !ok || string(restored.Value) != "value" || restored.Version != record.Version {
		t.Fatalf("restored record = %+v, want value/version from %+v", restored, record)
	}
	if restored.ExpireAt.IsZero() {
		t.Fatal("restored record should preserve expire_at")
	}
}

func TestMemoryEngineSnapshotFileRoundTrip(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "orders", Space: "session", Key: "file"}
	if _, _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set record: %v", err)
	}
	snapshot, err := eng.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	path := filepath.Join(t.TempDir(), "node", "snapshot.json")
	if saveErr := engine.SaveSnapshotFile(path, snapshot); saveErr != nil {
		t.Fatalf("save snapshot file: %v", saveErr)
	}
	loaded, err := engine.LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("load snapshot file: %v", err)
	}
	if len(loaded.Entries) != 1 || string(loaded.Entries[0].Value) != "value" {
		t.Fatalf("loaded snapshot = %+v, want one value entry", loaded)
	}
}
