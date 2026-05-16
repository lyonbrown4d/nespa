package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

	setResults, err := svc.BatchSet(context.Background(), []cache.SetRequest{
		{Key: keys[0], Value: []byte("one")},
		{Key: keys[1], Value: []byte("two")},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(setResults) != 2 {
		t.Fatalf("records len = %d, want 2", len(setResults))
	}

	getResults, err := svc.BatchGet(context.Background(), []cache.GetRequest{
		{Key: keys[0]},
		{Key: keys[1]},
		{Key: cache.Key{Namespace: "order", Space: "session", Key: "missing"}},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(getResults) != 3 {
		t.Fatalf("results len = %d, want 3", len(getResults))
	}
	if !getResults[0].Found || string(getResults[0].Record.Value) != "one" {
		t.Fatalf("first result = %+v", getResults[0])
	}
	if !getResults[1].Found || string(getResults[1].Record.Value) != "two" {
		t.Fatalf("second result = %+v", getResults[1])
	}
	if getResults[2].Found {
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
	deleted, applied, err := svc.Delete(context.Background(), key, cache.DeleteOptions{})
	if err != nil || !deleted || !applied {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := svc.Set(context.Background(), key, []byte("12345"), cache.SetOptions{}); err != nil {
		t.Fatalf("set after delete: %v", err)
	}
}

func TestEngineServiceSetRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "order", Space: "session", Key: "k1"}

	original, err := svc.Set(context.Background(), key, []byte("initial"), cache.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	result, err := svc.Set(context.Background(), key, []byte("updated"), cache.SetOptions{
		ExpectedVersion: original.Record.Version + 1,
	})
	if err != nil {
		t.Fatalf("set with mismatched expected version: %v", err)
	}
	if result.Found {
		t.Fatal("set should not match on mismatched expected version")
	}

	if _, ok, err := svc.Get(context.Background(), key, cache.GetOptions{}); err != nil || !ok {
		t.Fatalf("record should remain: ok=%v err=%v", ok, err)
	}
}

func TestEngineServiceDeleteRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "order", Space: "session", Key: "k1"}

	set, err := svc.Set(context.Background(), key, []byte("initial"), cache.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	deleted, applied, err := svc.Delete(context.Background(), key, cache.DeleteOptions{
		ExpectedVersion: set.Record.Version + 1,
	})
	if err != nil {
		t.Fatalf("delete with mismatched expected version: %v", err)
	}
	if deleted {
		t.Fatal("delete should not hit on mismatched expected version")
	}
	if applied {
		t.Fatal("delete should not apply on mismatched expected version")
	}

	if _, ok, err := svc.Get(context.Background(), key, cache.GetOptions{}); err != nil || !ok {
		t.Fatalf("record should remain: ok=%v err=%v", ok, err)
	}
}

func TestEngineServiceTouchRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "order", Space: "session", Key: "k1"}
	set, err := svc.Set(context.Background(), key, []byte("initial"), cache.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	touched, err := svc.Touch(context.Background(), key, cache.TouchOptions{
		ExpectedVersion: set.Record.Version + 1,
	})
	if err != nil {
		t.Fatalf("touch with mismatched expected version: %v", err)
	}
	if touched {
		t.Fatal("touch should miss on mismatched expected version")
	}
}

func TestEngineServiceAdjustIncrementsAndPreservesTTL(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "n", Space: "s", Key: "counter"}

	result, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{
		InitialValue: 10,
		Delta:        4,
		TTL:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("adjust create: %v", err)
	}
	if !result.Found {
		t.Fatalf("adjust should return found=true on create")
	}
	if string(result.Record.Value) != "14" {
		t.Fatalf("adjust create value = %q, want 14", result.Record.Value)
	}
	if result.Record.Version != 1 {
		t.Fatalf("adjust create version = %d, want 1", result.Record.Version)
	}
	if result.Record.ExpireAt.IsZero() {
		t.Fatal("adjust create should set expire at")
	}

	result, err = svc.Adjust(context.Background(), key, cache.AdjustOptions{
		Delta: -2,
	})
	if err != nil {
		t.Fatalf("adjust increment: %v", err)
	}
	if !result.Found || string(result.Record.Value) != "12" {
		t.Fatalf("adjust increment = %+v", result)
	}
	if result.Record.Version != 2 {
		t.Fatalf("adjust increment version = %d, want 2", result.Record.Version)
	}
	if result.Record.ExpireAt.IsZero() {
		t.Fatal("adjust increment should keep expire at")
	}
}

func TestEngineServiceAdjustRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "n", Space: "s", Key: "counter"}
	seed, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{
		InitialValue: 1,
		Delta:        1,
	})
	if err != nil {
		t.Fatalf("seed adjust: %v", err)
	}

	result, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{
		Delta:           1,
		ExpectedVersion: seed.Record.Version + 1,
	})
	if err != nil {
		t.Fatalf("adjust with mismatched expected version: %v", err)
	}
	if result.Found {
		t.Fatal("adjust should miss on mismatched expected version")
	}

	record, ok, err := svc.Get(context.Background(), key, cache.GetOptions{})
	if err != nil {
		t.Fatalf("get after mismatch: %v", err)
	}
	if !ok {
		t.Fatal("record should exist after mismatched adjust")
	}
	if string(record.Value) != "2" {
		t.Fatalf("record value = %q, want 2", record.Value)
	}
}

func TestEngineServiceAdjustRejectsNonIntExistingValue(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "n", Space: "s", Key: "counter"}

	if _, err := svc.Set(context.Background(), key, []byte("bad-int"), cache.SetOptions{}); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	_, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{Delta: 1})
	if err == nil {
		t.Fatal("adjust should fail for non-int value")
	}
	if !errors.Is(err, engine.ErrInvalidCounter) {
		t.Fatalf("error = %v, want ErrInvalidCounter", err)
	}
}

func TestEngineServiceAdjustRejectsOverflow(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "n", Space: "s", Key: "counter"}
	maxInt64 := int64(^uint64(0) >> 1)

	_, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{
		InitialValue: maxInt64,
		Delta:        1,
	})
	if err == nil {
		t.Fatal("adjust should fail for overflow value")
	}
	if !errors.Is(err, engine.ErrInvalidCounter) {
		t.Fatalf("error = %v, want ErrInvalidCounter", err)
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
