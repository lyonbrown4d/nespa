package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/internal/node/engine"
)

func TestEngineServiceBatchSetGet(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer eng.Close()

	svc := NewService(eng)
	keys := []Key{
		{Namespace: "order", Space: "session", Key: "a"},
		{Namespace: "order", Space: "session", Key: "b"},
	}

	records, err := svc.BatchSet(context.Background(), []SetRequest{
		{Key: keys[0], Value: []byte("one")},
		{Key: keys[1], Value: []byte("two")},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records len = %d, want 2", len(records))
	}

	results, err := svc.BatchGet(context.Background(), []GetRequest{
		{Key: keys[0]},
		{Key: keys[1]},
		{Key: Key{Namespace: "order", Space: "session", Key: "missing"}},
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
	defer eng.Close()

	svc := NewService(eng, WithQuota(QuotaConfig{DefaultSpaceMemoryBytes: 10}))
	key := Key{Namespace: "order", Space: "session", Key: "a"}

	_, err := svc.Set(context.Background(), key, []byte("value"), SetOptions{})
	if err == nil {
		t.Fatal("set succeeded, want quota error")
	}
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEngineServiceSpaceQuotaEvictsSameSpace(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer eng.Close()

	oldKey := Key{Namespace: "n", Space: "s", Key: "old"}
	newKey := Key{Namespace: "n", Space: "s", Key: "new"}
	otherKey := Key{Namespace: "n", Space: "other", Key: "o"}
	oldValue := []byte("old")
	newValue := []byte("new")
	otherValue := []byte("1")
	limit := engine.EstimateCost(oldKey, oldValue) + engine.EstimateCost(newKey, newValue) - 1
	svc := NewService(eng, WithQuota(QuotaConfig{DefaultSpaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), oldKey, oldValue, SetOptions{}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	if _, err := svc.Set(context.Background(), otherKey, otherValue, SetOptions{}); err != nil {
		t.Fatalf("set other: %v", err)
	}
	if _, err := svc.Set(context.Background(), newKey, newValue, SetOptions{}); err != nil {
		t.Fatalf("set new after eviction: %v", err)
	}

	if _, ok, err := svc.Get(context.Background(), oldKey, GetOptions{}); err != nil || ok {
		t.Fatalf("old key after eviction: ok=%v err=%v", ok, err)
	}
	if _, ok, err := svc.Get(context.Background(), newKey, GetOptions{}); err != nil || !ok {
		t.Fatalf("new key after eviction: ok=%v err=%v", ok, err)
	}
	if _, ok, err := svc.Get(context.Background(), otherKey, GetOptions{}); err != nil || !ok {
		t.Fatalf("other key after eviction: ok=%v err=%v", ok, err)
	}
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
	defer eng.Close()

	key := Key{Namespace: "n", Space: "s", Key: "k"}
	svc := NewService(eng, WithQuota(QuotaConfig{DefaultSpaceMemoryBytes: engine.EstimateCost(key, []byte("1")) - 1}))

	if _, err := svc.Set(context.Background(), key, []byte("1"), SetOptions{}); err == nil {
		t.Fatal("set succeeded, want quota error")
	} else if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("set error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEngineServiceQuotaTracksDelete(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer eng.Close()

	key := Key{Namespace: "n", Space: "s", Key: "k"}
	limit := engine.EstimateCost(key, []byte("12345")) + 1
	svc := NewService(eng, WithQuota(QuotaConfig{DefaultSpaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), key, []byte("12345"), SetOptions{}); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if deleted, err := svc.Delete(context.Background(), key); err != nil || !deleted {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := svc.Set(context.Background(), key, []byte("12345"), SetOptions{}); err != nil {
		t.Fatalf("set after delete: %v", err)
	}
}

func TestEngineServiceNamespaceQuota(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer eng.Close()

	keyA := Key{Namespace: "n", Space: "a", Key: "k"}
	keyB := Key{Namespace: "n", Space: "b", Key: "k"}
	limit := engine.EstimateCost(keyA, []byte("1")) + engine.EstimateCost(keyB, []byte("1"))
	svc := NewService(eng, WithQuota(QuotaConfig{DefaultNamespaceMemoryBytes: limit}))

	if _, err := svc.Set(context.Background(), keyA, []byte("1"), SetOptions{}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if _, err := svc.Set(context.Background(), keyB, []byte("1"), SetOptions{}); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if _, err := svc.Set(context.Background(), Key{Namespace: "n", Space: "c", Key: "k"}, []byte("1"), SetOptions{}); err == nil {
		t.Fatal("set c succeeded, want namespace quota error")
	} else if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("set c error = %v, want ErrQuotaExceeded", err)
	}
}
