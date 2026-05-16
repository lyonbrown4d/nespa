package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestClientServerPrimitiveMap(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "profile"}

	set, err := client.Primitive(t.Context(), server.Addr(), cachewire.PrimitiveRequest{
		Kind:  cachewire.PrimitiveMapSet,
		Key:   key,
		Field: "name",
		Value: []byte("alice"),
	})
	if err != nil {
		t.Fatalf("primitive map set: %v", err)
	}
	if !set.Applied || set.Count != 1 {
		t.Fatalf("map set result = %+v", set)
	}

	got, err := client.Primitive(t.Context(), server.Addr(), cachewire.PrimitiveRequest{
		Kind:  cachewire.PrimitiveMapGet,
		Key:   key,
		Field: "name",
	})
	if err != nil {
		t.Fatalf("primitive map get: %v", err)
	}
	if !got.Found || string(got.Value) != "alice" {
		t.Fatalf("map get result = %+v", got)
	}
}

func TestClientServerBatchPrimitiveMixedOperations(t *testing.T) {
	server, client := startCacheClientServer(t)
	response, err := client.BatchPrimitive(t.Context(), server.Addr(), cachewire.BatchPrimitiveRequest{
		Items: []cachewire.PrimitiveRequest{
			{Kind: cachewire.PrimitiveCounterAdjust, Key: primitiveWireKey("counter"), Delta: 1},
			{Kind: cachewire.PrimitiveMapSet, Key: primitiveWireKey("profile"), Field: "name", Value: []byte("alice")},
			{Kind: cachewire.PrimitiveSetAdd, Key: primitiveWireKey("tags"), Member: "blue"},
			{Kind: cachewire.PrimitiveScoredSetPut, Key: primitiveWireKey("rank"), Member: "alice", Score: 10},
			{Kind: cachewire.PrimitiveMapGet, Key: primitiveWireKey("profile"), Field: "name"},
			{Kind: cachewire.PrimitiveSetContains, Key: primitiveWireKey("tags"), Member: "blue"},
			{Kind: cachewire.PrimitiveScoredSetRange, Key: primitiveWireKey("rank")},
			{Kind: cachewire.PrimitiveListPushBack, Key: primitiveWireKey("queue"), Value: []byte("middle")},
			{Kind: cachewire.PrimitiveListPushFront, Key: primitiveWireKey("queue"), Value: []byte("first")},
			{Kind: cachewire.PrimitiveListRange, Key: primitiveWireKey("queue")},
			{Kind: cachewire.PrimitiveListPopFront, Key: primitiveWireKey("queue")},
		},
	})
	if err != nil {
		t.Fatalf("batch primitive: %v", err)
	}
	requireBatchPrimitive(t, response)
}

func requireBatchPrimitive(t *testing.T, response cachewire.BatchPrimitiveResponse) {
	t.Helper()
	if len(response.Results) != 11 {
		t.Fatalf("result len = %d, want 11", len(response.Results))
	}
	requirePrimitiveScalarResults(t, response.Results)
	requirePrimitiveListResults(t, response.Results)
}

func requirePrimitiveScalarResults(t *testing.T, results []cachewire.PrimitiveResult) {
	t.Helper()
	if string(results[0].Value) != "1" {
		t.Fatalf("counter result = %+v", results[0])
	}
	if !results[4].Found || string(results[4].Value) != "alice" {
		t.Fatalf("map get result = %+v", results[4])
	}
	if !results[5].Bool {
		t.Fatalf("set contains result = %+v", results[5])
	}
	if len(results[6].ScoredMembers) != 1 || results[6].ScoredMembers[0].Member != "alice" {
		t.Fatalf("scored range result = %+v", results[6])
	}
}

func requirePrimitiveListResults(t *testing.T, results []cachewire.PrimitiveResult) {
	t.Helper()
	if len(results[9].Values) != 2 ||
		string(results[9].Values[0].Value) != "first" ||
		string(results[9].Values[1].Value) != "middle" {
		t.Fatalf("list range result = %+v", results[9])
	}
	if string(results[10].Value) != "first" || results[10].Count != 1 {
		t.Fatalf("list pop result = %+v", results[10])
	}
}

func primitiveWireKey(key string) cachewire.Key {
	return cachewire.Key{Namespace: "ns", Space: "sp", Key: key}
}
