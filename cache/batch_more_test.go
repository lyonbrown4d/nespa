package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServiceBatchDeleteExistsTouch(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	keys := []cache.Key{
		{Namespace: "n", Space: "s", Key: "a"},
		{Namespace: "n", Space: "s", Key: "b"},
	}
	setBatchValues(t, svc, keys)

	exists, err := svc.BatchExists(context.Background(), []cache.GetRequest{{Key: keys[0]}, {Key: keys[1]}})
	if err != nil {
		t.Fatalf("batch exists: %v", err)
	}
	requireBatchExistsResults(t, exists)

	touches, err := svc.BatchTouch(context.Background(), []cache.TouchRequest{
		{Key: keys[0], Options: cache.TouchOptions{TTL: time.Second}},
		{Key: cache.Key{Namespace: "n", Space: "s", Key: "missing"}, Options: cache.TouchOptions{TTL: time.Second}},
	})
	if err != nil {
		t.Fatalf("batch touch: %v", err)
	}
	requireBatchTouchResults(t, touches)

	deletes, err := svc.BatchDelete(context.Background(), []cache.DeleteRequest{{Key: keys[0]}, {Key: keys[1]}})
	if err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	requireBatchDeleteResults(t, deletes)
	requireMissing(t, svc, keys[0], "first deleted key")
	requireMissing(t, svc, keys[1], "second deleted key")
}

func requireBatchExistsResults(t *testing.T, exists []cache.ExistsResult) {
	t.Helper()
	if len(exists) != 2 || !exists[0].Exists || !exists[1].Exists {
		t.Fatalf("batch exists results = %+v", exists)
	}
}

func requireBatchTouchResults(t *testing.T, touches []cache.TouchResult) {
	t.Helper()
	if len(touches) != 2 || !touches[0].Touched || touches[1].Touched {
		t.Fatalf("batch touch results = %+v", touches)
	}
}

func requireBatchDeleteResults(t *testing.T, deletes []cache.DeleteResult) {
	t.Helper()
	if len(deletes) != 2 || !deletes[0].Deleted || !deletes[1].Deleted {
		t.Fatalf("batch delete results = %+v", deletes)
	}
}

func setBatchValues(t *testing.T, svc cache.Service, keys []cache.Key) {
	t.Helper()
	_, err := svc.BatchSet(context.Background(), []cache.SetRequest{
		{Key: keys[0], Value: []byte("a")},
		{Key: keys[1], Value: []byte("b")},
	})
	if err != nil {
		t.Fatalf("batch set values: %v", err)
	}
}
