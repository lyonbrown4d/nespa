package client_test

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
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

func TestRoutedTCPClientRetriesOnlyUnsentBatchGroups(t *testing.T) {
	firstEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, firstEngine)

	secondEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, secondEngine)

	var firstEpoch atomic.Uint64
	firstEpoch.Store(1)
	firstNode := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: firstEpoch.Load,
	}, cache.NewService(firstEngine))
	if err := firstNode.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start first cache server: %v", err)
	}
	defer stopServer(t, firstNode)

	var secondEpoch atomic.Uint64
	secondEpoch.Store(1)
	secondNode := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: secondEpoch.Load,
	}, cache.NewService(secondEngine))
	if err := secondNode.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start second cache server: %v", err)
	}
	defer stopServer(t, secondNode)

	control := newSnapshotServer(t, snapshotForRoutes(1, 1, []controlapi.RouteBody{
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 0,
			VSlotEnd:   32767,
			NodeID:     "node-first",
			Addr:       firstNode.Addr(),
		},
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 32768,
			VSlotEnd:   65535,
			NodeID:     "node-second",
			Addr:       secondNode.Addr(),
		},
	}))
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}

	refreshErr := routed.Refresh(t.Context())
	if refreshErr != nil {
		t.Fatalf("initial refresh: %v", refreshErr)
	}

	keyFirst := keyForVSlotRange(t, "orders", "session", 0, 32767)
	keySecond := keyForVSlotRange(t, "orders", "session", 32768, 65535)
	if keyFirst == keySecond {
		t.Fatalf("generated duplicate test keys")
	}
	routes := []controlapi.RouteBody{
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 0,
			VSlotEnd:   32767,
			NodeID:     "node-first",
			Addr:       firstNode.Addr(),
		},
		{
			Namespace:  "orders",
			Space:      "session",
			VSlotStart: 32768,
			VSlotEnd:   65535,
			NodeID:     "node-second",
			Addr:       secondNode.Addr(),
		},
	}
	firstRoute, ok := routing.Select(routes, "orders", "session", keyFirst)
	if !ok || firstRoute.NodeID != "node-first" {
		t.Fatalf("first key routed to unexpected node: %+v", firstRoute)
	}
	secondRoute, ok := routing.Select(routes, "orders", "session", keySecond)
	if !ok || secondRoute.NodeID != "node-second" {
		t.Fatalf("second key routed to unexpected node: %+v", secondRoute)
	}

	firstAddr := firstNode.Addr()
	secondAddr := secondNode.Addr()
	staleFirstNode := firstAddr > secondAddr
	if staleFirstNode {
		firstEpoch.Store(2)
	} else {
		secondEpoch.Store(2)
	}
	control.Set(snapshotForRoutes(2, 2, routes))

	probeClient := cachetcp.NewClient()
	_, probeErr := probeClient.BatchSet(t.Context(), secondNode.Addr(), cachewire.BatchSetRequest{
		RouteEpoch: 1,
		Items: []cachewire.SetRequest{
			{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "probe"}, Value: []byte("probe")},
		},
	})
	t.Logf("stale probe err=%v", probeErr)

	response, err := routed.BatchSet(t.Context(), cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: keyFirst}, Value: []byte("first")},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: keySecond}, Value: []byte("second")},
	}})
	if err != nil {
		t.Fatalf("batch set with partial stale route retry: %v", err)
	}
	if len(response.Records) != 2 {
		t.Fatalf("records len = %d, want 2", len(response.Records))
	}

	firstClient := cachetcp.NewClient()
	firstGet, err := firstClient.Get(t.Context(), firstNode.Addr(), cachewire.GetRequest{
		Key: cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       keyFirst,
		},
	})
	if err != nil {
		t.Fatalf("get first node record: %v", err)
	}
	if !firstGet.Found {
		t.Fatalf("first key should be found")
	}

	secondClient := cachetcp.NewClient()
	secondGet, err := secondClient.Get(t.Context(), secondNode.Addr(), cachewire.GetRequest{
		Key: cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       keySecond,
		},
	})
	if err != nil {
		t.Fatalf("get second node record: %v", err)
	}
	if !secondGet.Found {
		t.Fatalf("second key should be found")
	}

	firstForSecond, _ := firstClient.Get(t.Context(), firstNode.Addr(), cachewire.GetRequest{
		Key: cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       keySecond,
		},
	})
	secondForFirst, _ := secondClient.Get(t.Context(), secondNode.Addr(), cachewire.GetRequest{
		Key: cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       keyFirst,
		},
	})
	t.Logf("cross-check firstNode has keySecond found=%v version=%d, secondNode has keyFirst found=%v version=%d", firstForSecond.Found, firstForSecond.Version, secondForFirst.Found, secondForFirst.Version)

	if firstForSecond.Found || secondForFirst.Found {
		t.Fatalf("cross-node writes should not occur during routed batch")
	}

	firstNamespaceVersion := firstGet.NamespaceVersion
	secondNamespaceVersion := secondGet.NamespaceVersion
	t.Logf(
		"firstAddr=%s staleFirst=%v secondAddr=%s firstNamespace=%d secondNamespace=%d firstEpoch=%d secondEpoch=%d",
		firstAddr, staleFirstNode, secondAddr, firstNamespaceVersion, secondNamespaceVersion, firstEpoch.Load(), secondEpoch.Load(),
	)

	if response.Records[0].NamespaceVersion > 2 || response.Records[1].NamespaceVersion > 2 {
		t.Fatalf("namespace version should not exceed 2")
	}

	firstKeyRefreshed := response.Records[0].NamespaceVersion == 2
	secondKeyRefreshed := response.Records[1].NamespaceVersion == 2
	if firstKeyRefreshed == secondKeyRefreshed {
		t.Fatalf("expected exactly one key to be retried with namespace version 2")
	}
	if response.Records[0].NamespaceVersion != firstNamespaceVersion {
		t.Fatalf("response[0] namespace version=%d, first node namespace version=%d", response.Records[0].NamespaceVersion, firstNamespaceVersion)
	}
	if response.Records[1].NamespaceVersion != secondNamespaceVersion {
		t.Fatalf("response[1] namespace version=%d, second node namespace version=%d", response.Records[1].NamespaceVersion, secondNamespaceVersion)
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

func keyForVSlotRange(t *testing.T, namespace, space string, start, end uint32) string {
	t.Helper()
	for seq := 0; seq < 1_000_000; seq++ {
		key := fmt.Sprintf("routed-key-%s-%d", namespace, seq)
		slot := routing.VSlotFor(namespace, space, key)
		if slot >= start && slot <= end {
			return key
		}
	}
	t.Fatalf("could not find key for vslot range %d-%d", start, end)
	return ""
}
