package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineStatsIncludesSpaceUsage(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	keyA := engine.Key{Namespace: "order", Space: "session", Key: "a"}
	keyB := engine.Key{Namespace: "order", Space: "view", Key: "b"}

	setEngineValue(t, eng, keyA, "one")
	setEngineValue(t, eng, keyB, "two")

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

func TestMemoryEngineStatsTrackGetAndTouchOutcomes(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 1, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	keys := seedStatsRecords(t, eng)
	exerciseStatsOutcomes(t, eng, &now, keys)

	stats, err := eng.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	assertStatsCounters(t, stats)
}

type statsKeys struct {
	hit   engine.Key
	touch engine.Key
}

func seedStatsRecords(t *testing.T, eng engine.Engine) statsKeys {
	t.Helper()

	keys := statsKeys{
		hit:   engine.Key{Namespace: "stats", Space: "cache", Key: "hit"},
		touch: engine.Key{Namespace: "stats", Space: "cache", Key: "touch"},
	}
	if _, _, err := eng.Set(context.Background(), keys.hit, []byte("value"), engine.SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set hit key: %v", err)
	}
	if _, _, err := eng.Set(context.Background(), keys.touch, []byte("touch"), engine.SetOptions{TTL: 5 * time.Second}); err != nil {
		t.Fatalf("set touch key: %v", err)
	}
	return keys
}

func exerciseStatsOutcomes(t *testing.T, eng engine.Engine, now *time.Time, keys statsKeys) {
	t.Helper()

	requireStatsGet(t, eng, engine.Key{Namespace: "stats", Space: "cache", Key: "missing"}, false, "missing key")
	requireStatsGet(t, eng, keys.hit, true, "hit key before ttl")

	*now = now.Add(2 * time.Second)
	requireStatsGet(t, eng, keys.hit, false, "expired hit key")
	requireStatsTouch(t, eng, keys.touch, true, "touch hit key")
	requireStatsTouch(t, eng, engine.Key{Namespace: "stats", Space: "cache", Key: "missing-touch"}, false, "missing touch key")
}

func assertStatsCounters(t *testing.T, stats engine.Stats) {
	t.Helper()

	assertUint64(t, "get requests", stats.GetRequests, 3)
	assertUint64(t, "get hits", stats.GetHits, 1)
	assertUint64(t, "get misses", stats.GetMisses, 2)
	assertUint64(t, "get expired", stats.GetExpired, 1)
	assertUint64(t, "touch requests", stats.TouchRequests, 2)
	assertUint64(t, "touch hits", stats.TouchHits, 1)
	assertUint64(t, "touch misses", stats.TouchMisses, 1)
}

func assertSpaceStat(t *testing.T, stat engine.SpaceStats, namespace, space string) {
	t.Helper()
	if stat.Namespace != namespace || stat.Space != space || stat.Objects != 1 {
		t.Fatalf("space stat = %+v, want %s/%s with 1 object", stat, namespace, space)
	}
}

func assertUint64(t *testing.T, name string, got, want uint64) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %d, want %d", name, got, want)
	}
}

func requireStatsGet(t *testing.T, eng engine.Engine, key engine.Key, wantFound bool, name string) {
	t.Helper()
	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || ok != wantFound {
		t.Fatalf("get %s: ok=%v want=%v err=%v", name, ok, wantFound, err)
	}
}

func requireStatsTouch(t *testing.T, eng engine.Engine, key engine.Key, wantTouched bool, name string) {
	t.Helper()
	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{TTL: 0})
	if err != nil || touched != wantTouched {
		t.Fatalf("touch %s: touched=%v want=%v err=%v", name, touched, wantTouched, err)
	}
}
