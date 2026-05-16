package cache_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServiceBatchPrimitiveMixedOperations(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	counter := cache.Key{Namespace: "n", Space: "s", Key: "counter"}
	profile := cache.Key{Namespace: "n", Space: "s", Key: "profile"}
	tags := cache.Key{Namespace: "n", Space: "s", Key: "tags"}
	rank := cache.Key{Namespace: "n", Space: "s", Key: "rank"}

	results, err := svc.BatchPrimitive(context.Background(), []cache.PrimitiveRequest{
		{Kind: cache.PrimitiveCounterAdjust, Key: counter, Delta: 1},
		{Kind: cache.PrimitiveMapSet, Key: profile, Field: "name", Value: []byte("alice")},
		{Kind: cache.PrimitiveSetAdd, Key: tags, Member: "blue"},
		{Kind: cache.PrimitiveScoredSetPut, Key: rank, Member: "alice", Score: 10},
		{Kind: cache.PrimitiveMapGet, Key: profile, Field: "name"},
		{Kind: cache.PrimitiveSetContains, Key: tags, Member: "blue"},
		{Kind: cache.PrimitiveScoredSetRange, Key: rank},
	})
	if err != nil {
		t.Fatalf("batch primitive: %v", err)
	}
	if len(results) != 7 {
		t.Fatalf("results len = %d, want 7", len(results))
	}
	assertBatchPrimitiveResults(t, results)
}

func assertBatchPrimitiveResults(t *testing.T, results []cache.PrimitiveResult) {
	t.Helper()
	assertCounterBatchResult(t, results[0])
	assertWriteBatchResults(t, results[1], results[2], results[3])
	assertReadBatchResults(t, results[4], results[5], results[6])
}

func assertCounterBatchResult(t *testing.T, result cache.PrimitiveResult) {
	t.Helper()
	if string(result.Value) != "1" {
		t.Fatalf("counter result = %+v", result)
	}
}

func assertWriteBatchResults(
	t *testing.T,
	mapSet cache.PrimitiveResult,
	setAdd cache.PrimitiveResult,
	scorePut cache.PrimitiveResult,
) {
	t.Helper()
	if !mapSet.Applied || !setAdd.Bool || !scorePut.Bool {
		t.Fatalf("write results = %+v %+v %+v", mapSet, setAdd, scorePut)
	}
}

func assertReadBatchResults(
	t *testing.T,
	mapGet cache.PrimitiveResult,
	setContains cache.PrimitiveResult,
	scoredRange cache.PrimitiveResult,
) {
	t.Helper()
	if !mapGet.Found || string(mapGet.Value) != "alice" {
		t.Fatalf("map get result = %+v", mapGet)
	}
	if !setContains.Found || !setContains.Bool {
		t.Fatalf("set contains result = %+v", setContains)
	}
	want := []cache.ScoredMember{{Member: "alice", Score: 10}}
	if len(scoredRange.ScoredMembers) != 1 || scoredRange.ScoredMembers[0] != want[0] {
		t.Fatalf("scored range = %+v, want %+v", scoredRange.ScoredMembers, want)
	}
}
