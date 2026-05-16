package client_test

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func startCacheNode(t *testing.T, eng *engine.MemoryEngine, cfg cachetcp.ServerConfig) *cachetcp.Server {
	t.Helper()
	server := cachetcp.NewServer(cfg, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start cache server: %v", err)
	}
	return server
}

func newRoutedClientForTest(t *testing.T, controlAddr string) *client.RoutedTCPClient {
	t.Helper()
	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: controlAddr})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}
	return routed
}

func refreshRoutedForTest(t *testing.T, routed *client.RoutedTCPClient) {
	t.Helper()
	if err := routed.Refresh(t.Context()); err != nil {
		t.Fatalf("refresh routed client: %v", err)
	}
}

func setRoutedRecord(t *testing.T, routed *client.RoutedTCPClient, key, value string) cachewire.Record {
	t.Helper()
	record, err := routed.Set(t.Context(), cachewire.SetRequest{
		Key:   cachewire.Key{Namespace: "orders", Space: "session", Key: key},
		Value: []byte(value),
	})
	if err != nil {
		t.Fatalf("set routed record: %v", err)
	}
	return record
}

func getNodeRecord(t *testing.T, node *cachetcp.Server, key string) cachewire.Record {
	t.Helper()
	record, err := cachetcp.NewClient().Get(t.Context(), node.Addr(), cachewire.GetRequest{
		Key: cachewire.Key{Namespace: "orders", Space: "session", Key: key},
	})
	if err != nil {
		t.Fatalf("get node record: %v", err)
	}
	return record
}

func requireWireRecordValue(t *testing.T, record cachewire.Record, want, name string) {
	t.Helper()
	if !record.Found || string(record.Value) != want {
		t.Fatalf("%s = %+v, want value %q", name, record, want)
	}
}

func splitRoutes(firstAddr, secondAddr string) []controlapi.RouteBody {
	return []controlapi.RouteBody{
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 0,
			VSlotEnd:   32767,
			NodeID:     "node-first",
			Addr:       firstAddr,
		},
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 32768,
			VSlotEnd:   controlapi.VSlotMax,
			NodeID:     "node-second",
			Addr:       secondAddr,
		},
	}
}

func singleRoute(addr string) []controlapi.RouteBody {
	return []controlapi.RouteBody{
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 0,
			VSlotEnd:   controlapi.VSlotMax,
			NodeID:     "node-first",
			Addr:       addr,
		},
	}
}

func snapshotForRoutes(revision, version uint64, routes []controlapi.RouteBody) controlapi.SnapshotBody {
	return controlapi.SnapshotBody{
		Revision: revision,
		Namespaces: []controlapi.NamespaceBody{
			{Namespace: "orders", Version: version},
		},
		Spaces: []controlapi.SpaceBody{
			{Namespace: "orders", Space: "session", Version: version},
		},
		Routes: routes,
	}
}

func keyForVSlotRange(t *testing.T, start, end uint32) string {
	t.Helper()
	for seq := range 1_000_000 {
		key := fmt.Sprintf("routed-key-orders-%d", seq)
		slot := routing.VSlotFor("orders", "session", key)
		if slot >= start && slot <= end {
			return key
		}
	}
	t.Fatalf("could not find key for vslot range %d-%d", start, end)
	return ""
}
