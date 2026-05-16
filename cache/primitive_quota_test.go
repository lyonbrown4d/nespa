package cache_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServicePrimitiveQuotaRejectsOversizedWrite(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 1}))
	key := cache.Key{Namespace: "n", Space: "s", Key: "profile"}

	_, err := svc.Primitive(context.Background(), cache.PrimitiveRequest{
		Kind:  cache.PrimitiveMapSet,
		Key:   key,
		Field: "name",
		Value: []byte("alice"),
	})
	if err == nil {
		t.Fatal("primitive write succeeded, want quota error")
	}
	if !errors.Is(err, cache.ErrQuotaExceeded) {
		t.Fatalf("error = %v, want ErrQuotaExceeded", err)
	}
	requireMissing(t, svc, key, "primitive key after quota rejection")
}

func TestEngineServicePrimitiveQuotaSkipsUnappliedWrite(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng, cache.WithQuota(cache.QuotaConfig{DefaultSpaceMemoryBytes: 1}))
	result, err := svc.Primitive(context.Background(), cache.PrimitiveRequest{
		Kind:  cache.PrimitiveMapSet,
		Key:   cache.Key{Namespace: "n", Space: "s", Key: "profile"},
		Field: "name",
		Value: []byte("alice"),
		Options: cache.PrimitiveOptions{
			ExpectedVersion: 1,
		},
	})
	if err != nil {
		t.Fatalf("primitive write with stale expected version: %v", err)
	}
	if result.Applied {
		t.Fatalf("result applied = true, want false: %+v", result)
	}
}
