package control_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestMigrationExecutorMovesPlannedRange(t *testing.T) {
	source := startMigrationCacheServer(t)
	target := startMigrationCacheServer(t)
	client := cachetcp.NewClient()
	svc := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Migration: control.MigrationConfig{
			Enabled:       true,
			SweepInterval: 10 * time.Millisecond,
			TaskTimeout:   time.Second,
		},
	})

	if _, err := svc.RegisterNode(t.Context(), "node-1", source.Addr()); err != nil {
		t.Fatalf("register source node: %v", err)
	}
	if _, err := svc.CreateNamespace(t.Context(), "orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := svc.CreateSpace(t.Context(), "orders", "session"); err != nil {
		t.Fatalf("create space: %v", err)
	}

	key := keyInMigrationHalf(t)
	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("migrated"),
	}); err != nil {
		t.Fatalf("seed source value: %v", err)
	}
	if _, err := svc.RegisterNode(t.Context(), "node-2", target.Addr()); err != nil {
		t.Fatalf("register target node: %v", err)
	}

	if err := control.StartMigrationExecutor(t.Context(), slog.New(slog.DiscardHandler), svc); err != nil {
		t.Fatalf("start migration executor: %v", err)
	}
	waitForMigrationPlanState(t, svc, "done")
	requireCacheValue(t, client, target.Addr(), key, "migrated")
	requireCacheMissing(t, client, source.Addr(), key)
}

func startMigrationCacheServer(t *testing.T) *cachetcp.Server {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeMigrationEngine(t, eng) })
	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start cache tcp server: %v", err)
	}
	t.Cleanup(func() { stopMigrationServer(t, server) })
	return server
}

func keyInMigrationHalf(t *testing.T) cachewire.Key {
	t.Helper()
	for index := range 100_000 {
		key := cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Entity:    "SessionView",
			Key:       fmt.Sprintf("migrate-%d", index),
		}
		slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
		if slot >= 32768 && slot <= controlapi.VSlotMax {
			return key
		}
	}
	t.Fatal("failed to find migration key")
	return cachewire.Key{}
}

func waitForMigrationPlanState(t *testing.T, svc *control.ServiceRuntime, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		plans := svc.MigrationPlans()
		if len(plans.Plans) == 1 && plans.Plans[0].State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("migration plans = %+v, want state %q", svc.MigrationPlans(), want)
}

func requireCacheValue(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key, want string) {
	t.Helper()
	record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get cache value: %v", err)
	}
	if !record.Found || string(record.Value) != want {
		t.Fatalf("record = found %v value %q, want %q", record.Found, record.Value, want)
	}
}

func requireCacheMissing(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key) {
	t.Helper()
	record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get missing cache value: %v", err)
	}
	if record.Found {
		t.Fatalf("record should be missing: %+v", record)
	}
}

func stopMigrationServer(t *testing.T, server *cachetcp.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("stop cache tcp server: %v", err)
	}
}

func closeMigrationEngine(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}
