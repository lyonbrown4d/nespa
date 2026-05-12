package engine

import (
	"context"
	"testing"
	"time"
)

func TestMemoryEngineSetGetCopiesValue(t *testing.T) {
	eng := NewMemory(Config{ShardCount: 4})
	defer eng.Close()
	key := Key{Namespace: "order", Space: "session", Key: "k1"}
	value := []byte("first")

	rec, err := eng.Set(context.Background(), key, value, SetOptions{})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if rec.Version != 1 {
		t.Fatalf("version = %d, want 1", rec.Version)
	}

	value[0] = 'x'
	got, ok, err := eng.Get(context.Background(), key, GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get miss")
	}
	if string(got.Value) != "first" {
		t.Fatalf("value = %q, want first", got.Value)
	}

	got.Value[0] = 'y'
	again, ok, err := eng.Get(context.Background(), key, GetOptions{})
	if err != nil {
		t.Fatalf("get again: %v", err)
	}
	if !ok || string(again.Value) != "first" {
		t.Fatalf("stored value mutated: ok=%v value=%q", ok, again.Value)
	}
}

func TestMemoryEngineTTL(t *testing.T) {
	eng := NewMemory(Config{ShardCount: 2})
	defer eng.Close()
	now := time.UnixMilli(1000)
	eng.now = func() time.Time { return now }

	key := Key{Namespace: "order", Space: "session", Key: "k1"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, ok, err := eng.Get(context.Background(), key, GetOptions{}); err != nil || !ok {
		t.Fatalf("get before ttl: ok=%v err=%v", ok, err)
	}

	now = now.Add(time.Second)
	if _, ok, err := eng.Get(context.Background(), key, GetOptions{}); err != nil || ok {
		t.Fatalf("get after ttl: ok=%v err=%v", ok, err)
	}

	stats, err := eng.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Objects != 0 {
		t.Fatalf("objects after lazy expiration = %d, want 0", stats.Objects)
	}
}

func TestMemoryEngineVersionVisibility(t *testing.T) {
	eng := NewMemory(Config{})
	defer eng.Close()
	key := Key{Namespace: "order", Space: "view", Key: "k1"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), SetOptions{
		NamespaceVersion: 2,
		SpaceVersion:     5,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, ok, err := eng.Get(context.Background(), key, GetOptions{NamespaceVersion: 1}); err != nil || ok {
		t.Fatalf("namespace version mismatch: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), key, GetOptions{SpaceVersion: 4}); err != nil || ok {
		t.Fatalf("space version mismatch: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), key, GetOptions{NamespaceVersion: 2, SpaceVersion: 5}); err != nil || !ok {
		t.Fatalf("version match: ok=%v err=%v", ok, err)
	}
}

func TestMemoryEngineDeleteAndStats(t *testing.T) {
	eng := NewMemory(Config{ShardCount: 4})
	defer eng.Close()
	key := Key{Namespace: "order", Space: "session", Key: "k1"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), SetOptions{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if stats, err := eng.Stats(context.Background()); err != nil || stats.Objects != 1 || stats.MemoryBytes == 0 {
		t.Fatalf("stats after set = %+v", stats)
	}

	deleted, err := eng.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete miss")
	}
	if stats, err := eng.Stats(context.Background()); err != nil || stats.Objects != 0 || stats.MemoryBytes != 0 {
		t.Fatalf("stats after delete = %+v", stats)
	}
}

func TestMemoryEngineStatsIncludesSpaceUsage(t *testing.T) {
	eng := NewMemory(Config{ShardCount: 4})
	defer eng.Close()

	keyA := Key{Namespace: "order", Space: "session", Key: "a"}
	keyB := Key{Namespace: "order", Space: "view", Key: "b"}

	if _, err := eng.Set(context.Background(), keyA, []byte("one"), SetOptions{}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if _, err := eng.Set(context.Background(), keyB, []byte("two"), SetOptions{}); err != nil {
		t.Fatalf("set b: %v", err)
	}

	stats, err := eng.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(stats.Spaces) != 2 {
		t.Fatalf("spaces len = %d, want 2: %+v", len(stats.Spaces), stats.Spaces)
	}
	if stats.Spaces[0].Namespace != "order" || stats.Spaces[0].Space != "session" || stats.Spaces[0].Objects != 1 {
		t.Fatalf("first space = %+v", stats.Spaces[0])
	}
	if stats.Spaces[1].Namespace != "order" || stats.Spaces[1].Space != "view" || stats.Spaces[1].Objects != 1 {
		t.Fatalf("second space = %+v", stats.Spaces[1])
	}
}

func TestMemoryEngineEvictsScopedLRU(t *testing.T) {
	eng := NewMemory(Config{ShardCount: 1})
	defer eng.Close()

	now := time.UnixMilli(1000)
	eng.now = func() time.Time { return now }

	oldKey := Key{Namespace: "n", Space: "s", Key: "old"}
	newKey := Key{Namespace: "n", Space: "s", Key: "new"}
	otherKey := Key{Namespace: "n", Space: "other", Key: "other"}

	if _, err := eng.Set(context.Background(), oldKey, []byte("old-value"), SetOptions{}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	now = now.Add(time.Second)
	if _, err := eng.Set(context.Background(), newKey, []byte("new-value"), SetOptions{}); err != nil {
		t.Fatalf("set new: %v", err)
	}
	if _, err := eng.Set(context.Background(), otherKey, []byte("other-value"), SetOptions{}); err != nil {
		t.Fatalf("set other: %v", err)
	}

	result, err := eng.Evict(context.Background(), EvictOptions{
		Namespace:   "n",
		Space:       "s",
		TargetBytes: engineCostForTest(oldKey, []byte("old-value")),
	})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if result.EvictedObjects != 1 {
		t.Fatalf("evicted objects = %d, want 1", result.EvictedObjects)
	}
	if _, ok, err := eng.Get(context.Background(), oldKey, GetOptions{}); err != nil || ok {
		t.Fatalf("old key after evict: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), newKey, GetOptions{}); err != nil || !ok {
		t.Fatalf("new key after evict: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), otherKey, GetOptions{}); err != nil || !ok {
		t.Fatalf("other key after evict: ok=%v err=%v", ok, err)
	}
}

func engineCostForTest(key Key, value []byte) uint64 {
	return EstimateCost(key, value)
}
