package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

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
	assertAdjustRecord(t, result, "14", 1, true, "create")

	result, err = svc.Adjust(context.Background(), key, cache.AdjustOptions{Delta: -2})
	if err != nil {
		t.Fatalf("adjust increment: %v", err)
	}
	assertAdjustRecord(t, result, "12", 2, true, "increment")
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
	requireServiceValue(t, svc, key, "2")
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

func assertAdjustRecord(t *testing.T, result cache.SetResult, value string, version uint64, wantTTL bool, name string) {
	t.Helper()
	if !result.Found {
		t.Fatalf("adjust %s should return found=true", name)
	}
	if string(result.Record.Value) != value {
		t.Fatalf("adjust %s value = %q, want %s", name, result.Record.Value, value)
	}
	if result.Record.Version != version {
		t.Fatalf("adjust %s version = %d, want %d", name, result.Record.Version, version)
	}
	if result.Record.ExpireAt.IsZero() == wantTTL {
		t.Fatalf("adjust %s ttl presence = %v, want %v", name, !result.Record.ExpireAt.IsZero(), wantTTL)
	}
}

func requireServiceValue(t *testing.T, svc cache.Service, key cache.Key, want string) {
	t.Helper()
	record, ok, err := svc.Get(context.Background(), key, cache.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get after mismatch: ok=%v err=%v", ok, err)
	}
	if string(record.Value) != want {
		t.Fatalf("record value = %q, want %s", record.Value, want)
	}
}
