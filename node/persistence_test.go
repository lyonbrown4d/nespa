package node_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/node"
)

func TestNodeEngineSnapshotPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "snapshot.json")
	source := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, source)

	key := engine.Key{Namespace: "orders", Space: "session", Key: "persisted"}
	if _, _, err := source.Set(context.Background(), key, []byte("value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set source record: %v", err)
	}
	cfg := node.Config{SnapshotPath: path}
	logger := slog.New(slog.DiscardHandler)
	if err := node.SaveEngineSnapshot(context.Background(), logger, cfg, source); err != nil {
		t.Fatalf("save engine snapshot: %v", err)
	}

	target := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, target)
	if err := node.RestoreEngineSnapshot(context.Background(), logger, cfg, target); err != nil {
		t.Fatalf("restore engine snapshot: %v", err)
	}
	record, found, err := target.Get(context.Background(), key, engine.GetOptions{})
	if err != nil {
		t.Fatalf("get restored record: %v", err)
	}
	if !found || string(record.Value) != "value" {
		t.Fatalf("restored record = %+v, found=%v", record, found)
	}
}

func closeEngine(t *testing.T, eng engine.Engine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}
