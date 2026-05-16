package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineSetGetCopiesValue(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)
	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	value := []byte("first")

	rec, _, err := eng.Set(context.Background(), key, value, engine.SetOptions{})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if rec.Version != 1 {
		t.Fatalf("version = %d, want 1", rec.Version)
	}

	value[0] = 'x'
	got, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
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
	again, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil {
		t.Fatalf("get again: %v", err)
	}
	if !ok || string(again.Value) != "first" {
		t.Fatalf("stored value mutated: ok=%v value=%q", ok, again.Value)
	}
}

func TestMemoryEnginePhysicalKeyEncodingAvoidsDelimiterCollision(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 1})
	defer closeEngine(t, eng)

	left := engine.Key{Namespace: "a", Space: "b\x00c", Key: "d"}
	right := engine.Key{Namespace: "a", Space: "b", Entity: "c\x00", Key: "d"}

	if _, _, err := eng.Set(context.Background(), left, []byte("left"), engine.SetOptions{}); err != nil {
		t.Fatalf("set left: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), right, []byte("right"), engine.SetOptions{}); err != nil {
		t.Fatalf("set right: %v", err)
	}

	gotLeft, ok, err := eng.Get(context.Background(), left, engine.GetOptions{})
	if err != nil || !ok || string(gotLeft.Value) != "left" {
		t.Fatalf("left value = %q ok=%v err=%v", gotLeft.Value, ok, err)
	}
	gotRight, ok, err := eng.Get(context.Background(), right, engine.GetOptions{})
	if err != nil || !ok || string(gotRight.Value) != "right" {
		t.Fatalf("right value = %q ok=%v err=%v", gotRight.Value, ok, err)
	}
}

func TestMemoryEngineTTL(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	if _, _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{}); err != nil || !ok {
		t.Fatalf("get before ttl: ok=%v err=%v", ok, err)
	}

	now = now.Add(time.Second)
	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{}); err != nil || ok {
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
	eng := engine.NewMemory(engine.Config{})
	defer closeEngine(t, eng)
	key := engine.Key{Namespace: "order", Space: "view", Key: "k1"}
	if _, _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{
		NamespaceVersion: 2,
		SpaceVersion:     5,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{NamespaceVersion: 1}); err != nil || ok {
		t.Fatalf("namespace version mismatch: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{SpaceVersion: 4}); err != nil || ok {
		t.Fatalf("space version mismatch: ok=%v err=%v", ok, err)
	}
	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{NamespaceVersion: 2, SpaceVersion: 5}); err != nil || !ok {
		t.Fatalf("version match: ok=%v err=%v", ok, err)
	}
}

func TestMemoryEngineSetRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	record, _, err := eng.Set(context.Background(), key, []byte("initial"), engine.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	_, found, err := eng.Set(context.Background(), key, []byte("updated"), engine.SetOptions{
		ExpectedVersion: record.Version + 1,
	})
	if err != nil {
		t.Fatalf("set with mismatched expected version: %v", err)
	}
	if found {
		t.Fatal("set with mismatched expected version should not match")
	}

	got, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get existing record: ok=%v err=%v", ok, err)
	}
	if string(got.Value) != "initial" {
		t.Fatalf("unexpected value = %q, want initial", got.Value)
	}
}

func TestMemoryEngineDeleteRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	record, _, err := eng.Set(context.Background(), key, []byte("initial"), engine.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	deleted, applied, err := eng.Delete(context.Background(), key, engine.DeleteOptions{
		ExpectedVersion: record.Version + 1,
	})
	if err != nil {
		t.Fatalf("delete with mismatched expected version: %v", err)
	}
	if deleted {
		t.Fatal("delete should report missing when expected version mismatched")
	}
	if applied {
		t.Fatal("delete with mismatched expected version should not apply")
	}

	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("record should still exist: ok=%v err=%v", ok, err)
	}
}

func TestMemoryEngineTouchRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	record, _, err := eng.Set(context.Background(), key, []byte("initial"), engine.SetOptions{})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{
		ExpectedVersion: record.Version + 1,
	})
	if err != nil {
		t.Fatalf("touch with mismatched expected version: %v", err)
	}
	if touched {
		t.Fatal("touch should not hit when expected version mismatched")
	}
}

func TestMemoryEngineDeleteAndStats(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)
	key := engine.Key{Namespace: "order", Space: "session", Key: "k1"}
	if _, _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if stats, err := eng.Stats(context.Background()); err != nil || stats.Objects != 1 || stats.MemoryBytes == 0 {
		t.Fatalf("stats after set = %+v", stats)
	}

	deleted, _, err := eng.Delete(context.Background(), key, engine.DeleteOptions{})
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

func TestMemoryEngineEvictsScopedLRU(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 1, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	oldKey := engine.Key{Namespace: "n", Space: "s", Key: "old"}
	newKey := engine.Key{Namespace: "n", Space: "s", Key: "new"}
	otherKey := engine.Key{Namespace: "n", Space: "other", Key: "other"}

	if _, _, err := eng.Set(context.Background(), oldKey, []byte("old-value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set old: %v", err)
	}
	now = now.Add(time.Second)
	if _, _, err := eng.Set(context.Background(), newKey, []byte("new-value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set new: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), otherKey, []byte("other-value"), engine.SetOptions{}); err != nil {
		t.Fatalf("set other: %v", err)
	}

	result, err := eng.Evict(context.Background(), engine.EvictOptions{
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
	requireEngineMissing(t, eng, oldKey, "old key after evict")
	requireEngineFound(t, eng, newKey, "new key after evict")
	requireEngineFound(t, eng, otherKey, "other key after evict")
}

func engineCostForTest(key engine.Key, value []byte) uint64 {
	return engine.EstimateCost(key, value)
}

func closeEngine(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func requireEngineMissing(t *testing.T, eng engine.Engine, key engine.Key, name string) {
	t.Helper()
	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || ok {
		t.Fatalf("%s: ok=%v err=%v", name, ok, err)
	}
}

func requireEngineFound(t *testing.T, eng engine.Engine, key engine.Key, name string) {
	t.Helper()
	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("%s: ok=%v err=%v", name, ok, err)
	}
}
