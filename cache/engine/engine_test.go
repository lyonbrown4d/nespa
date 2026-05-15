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

func TestMemoryEngineStatsIncludesSpaceUsage(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	keyA := engine.Key{Namespace: "order", Space: "session", Key: "a"}
	keyB := engine.Key{Namespace: "order", Space: "view", Key: "b"}

	if _, _, err := eng.Set(context.Background(), keyA, []byte("one"), engine.SetOptions{}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), keyB, []byte("two"), engine.SetOptions{}); err != nil {
		t.Fatalf("set b: %v", err)
	}

	stats, err := eng.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(stats.Spaces) != 2 {
		t.Fatalf("spaces len = %d, want 2: %+v", len(stats.Spaces), stats.Spaces)
	}
	assertSpaceStat(t, stats.Spaces[0], "order", "session")
	assertSpaceStat(t, stats.Spaces[1], "order", "view")
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

func TestMemoryEngineStatsTrackGetAndTouchOutcomes(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 1, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	keyHit := engine.Key{Namespace: "stats", Space: "cache", Key: "hit"}
	keyTouch := engine.Key{Namespace: "stats", Space: "cache", Key: "touch"}

	if _, _, err := eng.Set(context.Background(), keyHit, []byte("value"), engine.SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set hit key: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), keyTouch, []byte("touch"), engine.SetOptions{TTL: 5 * time.Second}); err != nil {
		t.Fatalf("set touch key: %v", err)
	}

	_, _, err := eng.Get(context.Background(), engine.Key{Namespace: "stats", Space: "cache", Key: "missing"}, engine.GetOptions{})
	if err != nil {
		t.Fatalf("get missing key failed: %v", err)
	}

	if _, ok, err := eng.Get(context.Background(), keyHit, engine.GetOptions{}); err != nil || !ok {
		t.Fatalf("get hit key before ttl: ok=%v err=%v", ok, err)
	}

	now = now.Add(2 * time.Second)
	if _, ok, err := eng.Get(context.Background(), keyHit, engine.GetOptions{}); err != nil || ok {
		t.Fatalf("get expired hit key: ok=%v err=%v", ok, err)
	}

	if _, err := eng.Touch(context.Background(), keyTouch, engine.TouchOptions{TTL: 0}); err != nil {
		t.Fatalf("touch hit key: %v", err)
	}

	touched, err := eng.Touch(context.Background(), engine.Key{Namespace: "stats", Space: "cache", Key: "missing-touch"}, engine.TouchOptions{TTL: 0})
	if err != nil {
		t.Fatalf("touch missing key failed: %v", err)
	}
	if touched {
		t.Fatal("touch missing key should not hit")
	}

	stats, err := eng.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.GetRequests != 3 {
		t.Fatalf("get requests = %d, want 3", stats.GetRequests)
	}
	if stats.GetHits != 1 {
		t.Fatalf("get hits = %d, want 1", stats.GetHits)
	}
	if stats.GetMisses != 2 {
		t.Fatalf("get misses = %d, want 2", stats.GetMisses)
	}
	if stats.GetExpired != 1 {
		t.Fatalf("get expired = %d, want 1", stats.GetExpired)
	}
	if stats.TouchRequests != 2 {
		t.Fatalf("touch requests = %d, want 2", stats.TouchRequests)
	}
	if stats.TouchHits != 1 {
		t.Fatalf("touch hits = %d, want 1", stats.TouchHits)
	}
	if stats.TouchMisses != 1 {
		t.Fatalf("touch misses = %d, want 1", stats.TouchMisses)
	}
}

func TestMemoryEngineDistinguishesEntitiesInPhysicalKeys(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 1})
	defer closeEngine(t, eng)

	keyA := engine.Key{Namespace: "ns", Space: "sp", Entity: "OrderView", Key: "k"}
	keyB := engine.Key{Namespace: "ns", Space: "sp", Entity: "SessionView", Key: "k"}

	if _, _, err := eng.Set(context.Background(), keyA, []byte("order"), engine.SetOptions{}); err != nil {
		t.Fatalf("set entity A: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), keyB, []byte("session"), engine.SetOptions{}); err != nil {
		t.Fatalf("set entity B: %v", err)
	}

	recordA, ok, err := eng.Get(context.Background(), keyA, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get entity A: ok=%v err=%v", ok, err)
	}
	if string(recordA.Value) != "order" {
		t.Fatalf("entity A value = %q, want %q", string(recordA.Value), "order")
	}

	recordB, ok, err := eng.Get(context.Background(), keyB, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get entity B: ok=%v err=%v", ok, err)
	}
	if string(recordB.Value) != "session" {
		t.Fatalf("entity B value = %q, want %q", string(recordB.Value), "session")
	}

	missingKey := keyA
	missingKey.Entity = "InvoiceView"
	_, ok, err = eng.Get(context.Background(), missingKey, engine.GetOptions{})
	if err != nil {
		t.Fatalf("get missing entity: %v", err)
	}
	if ok {
		t.Fatal("missing entity should not be found")
	}
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

func assertSpaceStat(t *testing.T, stat engine.SpaceStats, namespace, space string) {
	t.Helper()
	if stat.Namespace != namespace || stat.Space != space || stat.Objects != 1 {
		t.Fatalf("space stat = %+v, want %s/%s with 1 object", stat, namespace, space)
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
