package node_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

func TestRunSnapshotScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.DiscardHandler)

	var count atomic.Uint64
	done := make(chan struct{})

	go func() {
		node.RunSnapshotSchedulerWithFunc(ctx, log, 20*time.Millisecond, func(context.Context) error {
			count.Add(1)
			return nil
		})
		close(done)
	}()

	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		if count.Load() >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot scheduler did not trigger enough saves: got %d", count.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-done

	countAfterStop := count.Load()
	time.Sleep(60 * time.Millisecond)
	if got := count.Load(); got != countAfterStop {
		t.Fatalf("snapshot scheduler still running after stop: before=%d after=%d", countAfterStop, got)
	}
}

func TestRunSnapshotSchedulerDisabledOnZeroInterval(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)
	called := false

	node.RunSnapshotSchedulerWithFunc(ctx, log, 0, func(context.Context) error {
		called = true
		return errors.New("should not be called")
	})

	if called {
		t.Fatalf("zero interval should not execute save function")
	}
}

func closeEngine(t *testing.T, eng engine.Engine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}
