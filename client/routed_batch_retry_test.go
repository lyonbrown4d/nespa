package client_test

import (
	"sync/atomic"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestRoutedTCPClientRetriesDeadBatchGroupAfterRouteRefresh(t *testing.T) {
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

	control := newSnapshotServer(t, snapshotForRoutes(1, 1, splitRoutes(firstNode.Addr(), secondNode.Addr())))
	defer control.Close()

	routed := newRoutedClientForTest(t, control.URL)
	refreshRoutedForTest(t, routed)

	firstKey := keyForVSlotRange(t, 0, 32767)
	secondKey := keyForVSlotRange(t, 32768, controlapi.VSlotMax)
	stopServer(t, secondNode)
	firstEpoch.Store(2)
	control.Set(snapshotForRoutes(2, 2, singleRoute(firstNode.Addr())))

	response := batchSetRoutedRecords(t, routed, firstKey, secondKey)
	requireBatchSetRecordCount(t, response, 2)

	second := getNodeRecord(t, firstNode, secondKey)
	requireWireRecordValue(t, second, "second", "retried batch record")
}

func TestRoutedTCPClientRetriesOnlyUnsentBatchGroups(t *testing.T) {
	fixture := newRoutedBatchRetryFixture(t)
	keyFirst, keySecond := fixture.splitKeys(t)
	fixture.requireSplitRouting(t, keyFirst, keySecond)

	state := fixture.markOneNodeStale()
	response := batchSetRoutedRecords(t, fixture.routed, keyFirst, keySecond)
	requireBatchSetRecordCount(t, response, 2)

	writes := fixture.requireNodeWrites(t, keyFirst, keySecond)
	requireOnlyOneBatchRetry(t, response, writes, state)
}

type routedBatchRetryFixture struct {
	firstNode   *cachetcp.Server
	secondNode  *cachetcp.Server
	firstEpoch  *atomic.Uint64
	secondEpoch *atomic.Uint64
	control     *snapshotServer
	routed      *client.RoutedTCPClient
	routes      []controlapi.RouteBody
}

type routedBatchState struct {
	firstAddr      string
	secondAddr     string
	staleFirstNode bool
	firstEpoch     uint64
	secondEpoch    uint64
}

type routedBatchWrites struct {
	firstNamespaceVersion  uint64
	secondNamespaceVersion uint64
}

func newRoutedBatchRetryFixture(t *testing.T) *routedBatchRetryFixture {
	t.Helper()

	firstEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeEngine(t, firstEngine) })
	secondEngine := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeEngine(t, secondEngine) })

	firstEpoch := new(atomic.Uint64)
	firstEpoch.Store(1)
	firstNode := startCacheNode(t, firstEngine, cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: firstEpoch.Load,
	})
	t.Cleanup(func() { stopServer(t, firstNode) })

	secondEpoch := new(atomic.Uint64)
	secondEpoch.Store(1)
	secondNode := startCacheNode(t, secondEngine, cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: secondEpoch.Load,
	})
	t.Cleanup(func() { stopServer(t, secondNode) })

	routes := splitRoutes(firstNode.Addr(), secondNode.Addr())
	control := newSnapshotServer(t, snapshotForRoutes(1, 1, routes))
	t.Cleanup(control.Close)

	routed := newRoutedClientForTest(t, control.URL)
	refreshRoutedForTest(t, routed)

	return &routedBatchRetryFixture{
		firstNode:   firstNode,
		secondNode:  secondNode,
		firstEpoch:  firstEpoch,
		secondEpoch: secondEpoch,
		control:     control,
		routed:      routed,
		routes:      routes,
	}
}

func (f *routedBatchRetryFixture) splitKeys(t *testing.T) (string, string) {
	t.Helper()
	keyFirst := keyForVSlotRange(t, 0, 32767)
	keySecond := keyForVSlotRange(t, 32768, 65535)
	if keyFirst == keySecond {
		t.Fatalf("generated duplicate test keys")
	}
	return keyFirst, keySecond
}

func (f *routedBatchRetryFixture) requireSplitRouting(t *testing.T, keyFirst, keySecond string) {
	t.Helper()
	requireRouteNode(t, f.routes, keyFirst, "node-first")
	requireRouteNode(t, f.routes, keySecond, "node-second")
}

func (f *routedBatchRetryFixture) markOneNodeStale() routedBatchState {
	firstAddr := f.firstNode.Addr()
	secondAddr := f.secondNode.Addr()
	staleFirstNode := firstAddr > secondAddr
	if staleFirstNode {
		f.firstEpoch.Store(2)
	} else {
		f.secondEpoch.Store(2)
	}
	f.control.Set(snapshotForRoutes(2, 2, f.routes))
	return routedBatchState{
		firstAddr:      firstAddr,
		secondAddr:     secondAddr,
		staleFirstNode: staleFirstNode,
		firstEpoch:     f.firstEpoch.Load(),
		secondEpoch:    f.secondEpoch.Load(),
	}
}

func (f *routedBatchRetryFixture) requireNodeWrites(t *testing.T, keyFirst, keySecond string) routedBatchWrites {
	t.Helper()

	firstGet := getNodeRecord(t, f.firstNode, keyFirst)
	secondGet := getNodeRecord(t, f.secondNode, keySecond)
	firstForSecond := getNodeRecord(t, f.firstNode, keySecond)
	secondForFirst := getNodeRecord(t, f.secondNode, keyFirst)

	if !firstGet.Found {
		t.Fatalf("first key should be found")
	}
	if !secondGet.Found {
		t.Fatalf("second key should be found")
	}
	if firstForSecond.Found || secondForFirst.Found {
		t.Fatalf("cross-node writes should not occur during routed batch")
	}
	return routedBatchWrites{
		firstNamespaceVersion:  firstGet.NamespaceVersion,
		secondNamespaceVersion: secondGet.NamespaceVersion,
	}
}

func requireRouteNode(t *testing.T, routes []controlapi.RouteBody, key, wantNode string) {
	t.Helper()
	route, ok := routing.Select(routes, "orders", "session", key)
	if !ok || route.NodeID != wantNode {
		t.Fatalf("key %q routed to %+v, want %s", key, route, wantNode)
	}
}

func batchSetRoutedRecords(t *testing.T, routed *client.RoutedTCPClient, keyFirst, keySecond string) cachewire.BatchSetResponse {
	t.Helper()
	response, err := routed.BatchSet(t.Context(), cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: keyFirst}, Value: []byte("first")},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: keySecond}, Value: []byte("second")},
	}})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	return response
}

func requireBatchSetRecordCount(t *testing.T, response cachewire.BatchSetResponse, want int) {
	t.Helper()
	if len(response.Records) != want {
		t.Fatalf("records len = %d, want %d", len(response.Records), want)
	}
}

func requireOnlyOneBatchRetry(t *testing.T, response cachewire.BatchSetResponse, writes routedBatchWrites, state routedBatchState) {
	t.Helper()
	t.Logf(
		"firstAddr=%s staleFirst=%v secondAddr=%s firstNamespace=%d secondNamespace=%d firstEpoch=%d secondEpoch=%d",
		state.firstAddr, state.staleFirstNode, state.secondAddr,
		writes.firstNamespaceVersion, writes.secondNamespaceVersion, state.firstEpoch, state.secondEpoch,
	)

	if response.Records[0].NamespaceVersion > 2 || response.Records[1].NamespaceVersion > 2 {
		t.Fatalf("namespace version should not exceed 2")
	}

	firstKeyRefreshed := response.Records[0].NamespaceVersion == 2
	secondKeyRefreshed := response.Records[1].NamespaceVersion == 2
	if firstKeyRefreshed == secondKeyRefreshed {
		t.Fatalf("expected exactly one key to be retried with namespace version 2")
	}
	if response.Records[0].NamespaceVersion != writes.firstNamespaceVersion {
		t.Fatalf("response[0] namespace version=%d, first node namespace version=%d", response.Records[0].NamespaceVersion, writes.firstNamespaceVersion)
	}
	if response.Records[1].NamespaceVersion != writes.secondNamespaceVersion {
		t.Fatalf("response[1] namespace version=%d, second node namespace version=%d", response.Records[1].NamespaceVersion, writes.secondNamespaceVersion)
	}
}
