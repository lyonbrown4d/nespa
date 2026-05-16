package cache_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServiceSetQuotaSkipsUnappliedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := cache.Key{Namespace: "n", Space: "s", Key: "k"}
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 1}))

	result, err := svc.Set(context.Background(), key, []byte("oversized"), cache.SetOptions{
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatalf("set with stale expected version: %v", err)
	}
	if result.Found {
		t.Fatalf("set result applied = true, want false: %+v", result)
	}
}

func TestEngineServiceAdjustQuotaRejectsOversizedCounter(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 1}))
	_, err := svc.Adjust(context.Background(), cache.Key{
		Namespace: "n",
		Space:     "s",
		Key:       "counter",
	}, cache.AdjustOptions{InitialValue: 1})
	if err == nil {
		t.Fatal("adjust succeeded, want quota error")
	}
	if !errors.Is(err, cache.ErrQuotaExceeded) {
		t.Fatalf("adjust error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEngineServiceAdjustQuotaSkipsUnappliedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := cache.Key{Namespace: "n", Space: "s", Key: "counter"}
	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 1}))

	result, err := svc.Adjust(context.Background(), key, cache.AdjustOptions{
		Delta:           1,
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatalf("adjust with stale expected version: %v", err)
	}
	if result.Found {
		t.Fatalf("adjust result applied = true, want false: %+v", result)
	}
}
