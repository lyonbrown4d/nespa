package client_test

import (
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestRoutedTCPClientRefreshesAndRetriesStaleRouteEpoch(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	var routeEpoch atomic.Uint64
	routeEpoch.Store(1)
	data := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: routeEpoch.Load,
	}, cache.NewService(eng))
	if err := data.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, data)

	control := newSnapshotServer(t, snapshotFor(data.Addr(), 1, 1))
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}
	refreshErr := routed.Refresh(t.Context())
	if refreshErr != nil {
		t.Fatalf("initial refresh: %v", refreshErr)
	}

	routeEpoch.Store(2)
	control.Set(snapshotFor(data.Addr(), 2, 2))

	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "retry"}
	record, err := routed.Set(t.Context(), cachewire.SetRequest{Key: key, Value: []byte("v2")})
	if err != nil {
		t.Fatalf("set with stale route retry: %v", err)
	}
	if record.NamespaceVersion != 2 || record.SpaceVersion != 2 {
		t.Fatalf("record versions = %d/%d, want 2/2", record.NamespaceVersion, record.SpaceVersion)
	}
}

func TestRoutedTCPClientRefreshesAndRetriesStaleBatchRouteEpoch(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	var routeEpoch atomic.Uint64
	routeEpoch.Store(1)
	data := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: routeEpoch.Load,
	}, cache.NewService(eng))
	if err := data.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, data)

	control := newSnapshotServer(t, snapshotFor(data.Addr(), 1, 1))
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}
	refreshErr := routed.Refresh(t.Context())
	if refreshErr != nil {
		t.Fatalf("initial refresh: %v", refreshErr)
	}

	routeEpoch.Store(2)
	control.Set(snapshotFor(data.Addr(), 2, 2))

	response, err := routed.BatchSet(t.Context(), cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "a"}, Value: []byte("alpha")},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "b"}, Value: []byte("beta")},
	}})
	if err != nil {
		t.Fatalf("batch set with stale route retry: %v", err)
	}
	if len(response.Records) != 2 {
		t.Fatalf("records len = %d, want 2", len(response.Records))
	}
	for index := range response.Records {
		record := response.Records[index]
		if record.NamespaceVersion != 2 || record.SpaceVersion != 2 {
			t.Fatalf("record[%d] versions = %d/%d, want 2/2", index, record.NamespaceVersion, record.SpaceVersion)
		}
	}
}

func TestRoutedTCPClientRefreshesAfterDeadNodeDialError(t *testing.T) {
	firstEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, firstEngine)
	secondEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, secondEngine)

	var firstEpoch atomic.Uint64
	firstEpoch.Store(1)
	firstNode := startCacheNode(t, firstEngine, cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: firstEpoch.Load,
	})
	defer stopServer(t, firstNode)

	secondNode := startCacheNode(t, secondEngine, cachetcp.ServerConfig{Addr: "127.0.0.1:0"})
	defer stopServer(t, secondNode)

	routes := splitRoutes(firstNode.Addr(), secondNode.Addr())
	control := newSnapshotServer(t, snapshotForRoutes(1, 1, routes))
	defer control.Close()

	routed := newRoutedClientForTest(t, control.URL)
	refreshRoutedForTest(t, routed)

	key := keyForVSlotRange(t, 32768, controlapi.VSlotMax)
	stopServer(t, secondNode)
	firstEpoch.Store(2)
	control.Set(snapshotForRoutes(2, 2, singleRoute(firstNode.Addr())))

	record := setRoutedRecord(t, routed, key, "after-shrink")
	if !record.Found || record.NamespaceVersion != 2 || record.SpaceVersion != 2 {
		t.Fatalf("record after route shrink = %+v", record)
	}

	got := getNodeRecord(t, firstNode, key)
	requireWireRecordValue(t, got, "after-shrink", "surviving node record")
}
