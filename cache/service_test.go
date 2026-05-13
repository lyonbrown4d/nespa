package cache_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServiceBatchSetGet(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	keys := []cache.Key{
		{Namespace: "order", Space: "session", Key: "a"},
		{Namespace: "order", Space: "session", Key: "b"},
	}

	records, err := svc.BatchSet(context.Background(), []cache.SetRequest{
		{Key: keys[0], Value: []byte("one")},
		{Key: keys[1], Value: []byte("two")},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records len = %d, want 2", len(records))
	}

	results, err := svc.BatchGet(context.Background(), []cache.GetRequest{
		{Key: keys[0]},
		{Key: keys[1]},
		{Key: cache.Key{Namespace: "order", Space: "session", Key: "missing"}},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d, want 3", len(results))
	}
	if !results[0].Found || string(results[0].Record.Value) != "one" {
		t.Fatalf("first result = %+v", results[0])
	}
	if !results[1].Found || string(results[1].Record.Value) != "two" {
		t.Fatalf("second result = %+v", results[1])
	}
	if results[2].Found {
		t.Fatalf("missing result found = true")
	}
}

func TestEngineServiceSpaceQuotaRejectsOversizedWrite(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 10}))
	key := cache.Key{Namespace: "order", Space: "session", Key: "a"}

	_, err := svc.Set(context.Background(), key, []byte("value"), cache.SetOptions{})
	if err == nil {
		t.Fatal("set succeeded, want quota error")
	}
	if !errors.Is(err, cache.ErrQuotaExceeded) {
		t.Fatalf("error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEngineServiceSpaceQuotaEvictsSameSpace(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	oldKey := cache.Key{Namespace: "n", Space: "s", Key: "old"}
	newKey := cache.Key{Namespace: "n", Space: "s", Key: "new"}
	otherKey := cache.Key{Namespace: "n", Space: "other", Key: "o"}
	oldValue := []byte("old")
	newValue := []byte("new")
	otherValue := []byte("1")
	limit := engine.EstimateCost(oldKey, oldValue) + engine.EstimateCost(newKey, newValue) - 1
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), oldKey, oldValue, cache.SetOptions{}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	if _, err := svc.Set(context.Background(), otherKey, otherValue, cache.SetOptions{}); err != nil {
		t.Fatalf("set other: %v", err)
	}
	if _, err := svc.Set(context.Background(), newKey, newValue, cache.SetOptions{}); err != nil {
		t.Fatalf("set new after eviction: %v", err)
	}

	requireMissing(t, svc, oldKey, "old key after eviction")
	requireFound(t, svc, newKey, "new key after eviction")
	requireFound(t, svc, otherKey, "other key after eviction")
	stats, err := svc.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", stats.Evictions)
	}
}

func TestEngineServiceSpaceQuotaRejectsWhenEvictionInsufficient(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := cache.Key{Namespace: "n", Space: "s", Key: "k"}
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: engine.EstimateCost(key, []byte("1")) - 1}))

	if _, err := svc.Set(context.Background(), key, []byte("1"), cache.SetOptions{}); err == nil {
		t.Fatal("set succeeded, want quota error")
	} else if !errors.Is(err, cache.ErrQuotaExceeded) {
		t.Fatalf("set error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEngineServiceQuotaTracksDelete(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := cache.Key{Namespace: "n", Space: "s", Key: "k"}
	limit := engine.EstimateCost(key, []byte("12345")) + 1
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), key, []byte("12345"), cache.SetOptions{}); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if deleted, err := svc.Delete(context.Background(), key); err != nil || !deleted {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := svc.Set(context.Background(), key, []byte("12345"), cache.SetOptions{}); err != nil {
		t.Fatalf("set after delete: %v", err)
	}
}

func TestEngineServiceNamespaceQuota(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	keyA := cache.Key{Namespace: "n", Space: "a", Key: "k"}
	keyB := cache.Key{Namespace: "n", Space: "b", Key: "k"}
	limit := engine.EstimateCost(keyA, []byte("1")) + engine.EstimateCost(keyB, []byte("1"))
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultNamespaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), keyA, []byte("1"), cache.SetOptions{}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if _, err := svc.Set(context.Background(), keyB, []byte("1"), cache.SetOptions{}); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if _, err := svc.Set(context.Background(), cache.Key{Namespace: "n", Space: "c", Key: "k"}, []byte("1"), cache.SetOptions{}); err == nil {
		t.Fatal("set c succeeded, want namespace quota error")
	} else if !errors.Is(err, cache.ErrQuotaExceeded) {
		t.Fatalf("set c error = %v, want ErrQuotaExceeded", err)
	}
}

func closeEngine(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func requireMissing(t *testing.T, svc cache.Service, key cache.Key, name string) {
	t.Helper()
	_, ok, err := svc.Get(context.Background(), key, cache.GetOptions{})
	if err != nil || ok {
		t.Fatalf("%s: ok=%v err=%v", name, ok, err)
	}
}

func requireFound(t *testing.T, svc cache.Service, key cache.Key, name string) {
	t.Helper()
	_, ok, err := svc.Get(context.Background(), key, cache.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("%s: ok=%v err=%v", name, ok, err)
	}
}
