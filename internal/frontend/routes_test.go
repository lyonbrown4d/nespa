package frontend

import (
	"testing"

	"github.com/lyonbrown4d/nespa/internal/controlapi"
)

func TestRouteCacheSelectPrefersExactThenNamespaceThenWildcard(t *testing.T) {
	cache := NewRouteCache("test", []Route{
		{Role: "data-node", Addr: "wildcard"},
		{Namespace: "orders", Role: "data-node", Addr: "namespace"},
		{Namespace: "orders", Space: "session", Role: "data-node", Addr: "exact"},
	})

	exact, ok := cache.Select("orders", "session")
	if !ok || exact.Addr != "exact" {
		t.Fatalf("exact route = %+v, %v", exact, ok)
	}

	namespace, ok := cache.Select("orders", "view")
	if !ok || namespace.Addr != "namespace" {
		t.Fatalf("namespace route = %+v, %v", namespace, ok)
	}

	wildcard, ok := cache.Select("payments", "risk")
	if !ok || wildcard.Addr != "wildcard" {
		t.Fatalf("wildcard route = %+v, %v", wildcard, ok)
	}
}

func TestRouteCacheUpdateFromSnapshot(t *testing.T) {
	cache := NewRouteCache("bootstrap", []Route{{Role: "data-node", Addr: "bootstrap"}})
	updated := cache.UpdateFromSnapshot(controlapi.SnapshotBody{
		Revision: 7,
		Routes: []controlapi.RouteBody{
			{NodeID: "node-1", Addr: "127.0.0.1:7403", Weight: 1},
		},
	}, "control")
	if !updated {
		t.Fatal("expected route update")
	}

	snapshot := cache.Snapshot()
	if snapshot.RouteEpoch != 7 || snapshot.Source != "control" {
		t.Fatalf("unexpected snapshot metadata: %+v", snapshot)
	}
	if len(snapshot.Routes) != 1 || snapshot.Routes[0].NodeID != "node-1" {
		t.Fatalf("unexpected routes: %+v", snapshot.Routes)
	}
}

func TestRouteCacheIgnoresEmptySnapshotRoutes(t *testing.T) {
	cache := NewRouteCache("bootstrap", []Route{{Role: "data-node", Addr: "bootstrap"}})
	updated := cache.UpdateFromSnapshot(controlapi.SnapshotBody{Revision: 8}, "control")
	if updated {
		t.Fatal("expected empty snapshot to be ignored")
	}

	route, ok := cache.Select("orders", "session")
	if !ok || route.Addr != "bootstrap" {
		t.Fatalf("route = %+v, %v", route, ok)
	}
}
