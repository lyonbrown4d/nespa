package client_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestRoutedTCPClientBatchPrimitiveRoutesGroupsAndPreservesOrder(t *testing.T) {
	firstEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, firstEngine)
	secondEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, secondEngine)

	firstNode := startCacheNode(t, firstEngine, cachetcp.ServerConfig{Addr: "127.0.0.1:0"})
	defer stopServer(t, firstNode)
	secondNode := startCacheNode(t, secondEngine, cachetcp.ServerConfig{Addr: "127.0.0.1:0"})
	defer stopServer(t, secondNode)

	control := newSnapshotServer(t, snapshotForRoutes(1, 1, splitRoutes(firstNode.Addr(), secondNode.Addr())))
	defer control.Close()
	routed := newRoutedClientForTest(t, control.URL)
	refreshRoutedForTest(t, routed)

	firstKey := keyForVSlotRange(t, 0, 32767)
	secondKey := keyForVSlotRange(t, 32768, controlapi.VSlotMax)
	response := batchPrimitiveMapSets(t, routed, secondKey, firstKey)

	requireBatchPrimitiveKey(t, response.Results[0], secondKey)
	requireBatchPrimitiveKey(t, response.Results[1], firstKey)
	requireNodeMapField(t, secondNode, secondKey, "name", "second")
	requireNodeMapField(t, firstNode, firstKey, "name", "first")
}

func batchPrimitiveMapSets(
	t *testing.T,
	routed *client.RoutedTCPClient,
	firstRequestKey string,
	secondRequestKey string,
) cachewire.BatchPrimitiveResponse {
	t.Helper()
	response, err := routed.BatchPrimitive(t.Context(), cachewire.BatchPrimitiveRequest{
		Items: []cachewire.PrimitiveRequest{
			primitiveMapSet(firstRequestKey, "second"),
			primitiveMapSet(secondRequestKey, "first"),
		},
	})
	if err != nil {
		t.Fatalf("batch primitive: %v", err)
	}
	if len(response.Results) != 2 {
		t.Fatalf("results len = %d, want 2", len(response.Results))
	}
	return response
}

func primitiveMapSet(key, value string) cachewire.PrimitiveRequest {
	return cachewire.PrimitiveRequest{
		Key:   cachewire.Key{Namespace: "orders", Space: "session", Key: key},
		Kind:  cachewire.PrimitiveMapSet,
		Field: "name",
		Value: []byte(value),
	}
}

func requireBatchPrimitiveKey(t *testing.T, result cachewire.PrimitiveResult, key string) {
	t.Helper()
	if !result.Applied || result.Record.Key != key {
		t.Fatalf("primitive result = %+v, want applied key %q", result, key)
	}
}

func requireNodeMapField(t *testing.T, node *cachetcp.Server, key, field, want string) {
	t.Helper()
	result, err := cachetcp.NewClient().Primitive(t.Context(), node.Addr(), cachewire.PrimitiveRequest{
		Key:   cachewire.Key{Namespace: "orders", Space: "session", Key: key},
		Kind:  cachewire.PrimitiveMapGet,
		Field: field,
	})
	if err != nil {
		t.Fatalf("get node primitive field: %v", err)
	}
	if !result.Found || string(result.Value) != want {
		t.Fatalf("node primitive field = %+v, want %q", result, want)
	}
}
