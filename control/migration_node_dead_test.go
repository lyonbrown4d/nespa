package control

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/routing"
	"github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestMigrateRangeMovesWhenSourceNodeDies(t *testing.T) {
	source := startMigrationCacheServerForState(t)
	target := startMigrationCacheServerForState(t)
	client := tcp.NewClient()
	cfg := Config{
		ClusterID: "test",
		Migration: MigrationConfig{
			Enabled:       true,
			SweepInterval: 10 * time.Millisecond,
			TaskTimeout:   time.Second,
		},
	}
	svc := NewServiceRuntime(cfg)

	if _, err := svc.RegisterNode(t.Context(), "node-1", source.Addr()); err != nil {
		t.Fatalf("register source node: %v", err)
	}
	if _, err := svc.CreateNamespace(t.Context(), "orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := svc.CreateSpace(t.Context(), "orders", "session"); err != nil {
		t.Fatalf("create space: %v", err)
	}
	if _, err := svc.RegisterNode(t.Context(), "node-2", target.Addr()); err != nil {
		t.Fatalf("register target node: %v", err)
	}

	key := keyInSecondHalf(t)
	if _, err := client.Set(t.Context(), target.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("migrated"),
	}); err != nil {
		t.Fatalf("seed target value: %v", err)
	}

	if _, err := svc.Heartbeat(t.Context(), "node-1", source.Addr()); err != nil {
		t.Fatalf("heartbeat source node: %v", err)
	}

	svc.state.mu.Lock()
	node2, ok := svc.state.nodes.Get("node-2")
	if !ok {
		svc.state.mu.Unlock()
		t.Fatalf("expected node-2 in control state")
	}
	node2.LastSeenUnix = time.Now().Add(-10 * time.Second).Unix()
	svc.state.nodes.Set("node-2", node2)
	svc.state.mu.Unlock()

	if _, err := svc.advanceLiveness(t.Context(), time.Now(), 0, time.Second); err != nil {
		t.Fatalf("advance liveness: %v", err)
	}

	if err := StartMigrationExecutor(t.Context(), slog.New(slog.DiscardHandler), svc); err != nil {
		t.Fatalf("start migration executor: %v", err)
	}

	waitForKeyMoved(t, source.Addr(), target.Addr(), key, client)
}

func keyInSecondHalf(t *testing.T) cachewire.Key {
	t.Helper()
	for index := range 100_000 {
		k := cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Entity:    "SessionView",
			Key:       fmt.Sprintf("dead-migrate-%d", index),
		}
		slot := routing.VSlotFor(k.Namespace, k.Space, k.Key)
		if slot >= 32768 && slot <= 65535 {
			return k
		}
	}
	t.Fatal("failed to find key in second half vslot")
	return cachewire.Key{}
}

func waitForKeyMoved(
	t *testing.T,
	targetAddr string,
	sourceAddr string,
	key cachewire.Key,
	client *tcp.Client,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		targetRecord, targetErr := client.Get(t.Context(), targetAddr, cachewire.GetRequest{Key: key})
		sourceRecord, sourceErr := client.Get(t.Context(), sourceAddr, cachewire.GetRequest{Key: key})
		if targetErr == nil && sourceErr == nil &&
			targetRecord.Found && string(targetRecord.Value) == "migrated" &&
			!sourceRecord.Found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("migrated key not moved from source node to target node")
}

func startMigrationCacheServerForState(t *testing.T) *tcp.Server {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeMigrationEngineForStateTest(t, eng) })
	server := tcp.NewServer(tcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start cache tcp server: %v", err)
	}
	t.Cleanup(func() { stopMigrationServerForStateTest(t, server) })
	return server
}

func closeMigrationEngineForStateTest(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func stopMigrationServerForStateTest(t *testing.T, server *tcp.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("stop cache tcp server: %v", err)
	}
}
