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
		},
	})
	if err != nil {
		t.Fatalf("batch primitive: %v", err)
	}
	requireBatchPrimitive(t, response)
}

func requireBatchPrimitive(t *testing.T, response cachewire.BatchPrimitiveResponse) {
	t.Helper()
	if len(response.Results) != 7 {
		t.Fatalf("result len = %d, want 7", len(response.Results))
	}
	if string(response.Results[0].Value) != "1" {
		t.Fatalf("counter result = %+v", response.Results[0])
	}
	if !response.Results[4].Found || string(response.Results[4].Value) != "alice" {
		t.Fatalf("map get result = %+v", response.Results[4])
	}
	if !response.Results[5].Bool {
		t.Fatalf("set contains result = %+v", response.Results[5])
	}
	if len(response.Results[6].ScoredMembers) != 1 || response.Results[6].ScoredMembers[0].Member != "alice" {
		t.Fatalf("scored range result = %+v", response.Results[6])
	}
}

func primitiveWireKey(key string) cachewire.Key {
	return cachewire.Key{Namespace: "ns", Space: "sp", Key: key}
}
