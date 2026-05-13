package frontend_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/internal/controlapi"
	"github.com/lyonbrown4d/nespa/internal/frontend"
)

func TestRouteCacheSelectPrefersExactThenNamespaceThenWildcard(t *testing.T) {
	cache := frontend.NewRouteCache("test", []frontend.Route{
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
	cache := frontend.NewRouteCache("bootstrap", []frontend.Route{{Role: "data-node", Addr: "bootstrap"}})
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

func TestRouteCacheIgnoresInitialEmptySnapshotRoutes(t *testing.T) {
	cache := frontend.NewRouteCache("bootstrap", []frontend.Route{{Role: "data-node", Addr: "bootstrap"}})
	updated := cache.UpdateFromSnapshot(controlapi.SnapshotBody{Revision: 0}, "control")
	if updated {
		t.Fatal("expected initial empty snapshot to be ignored")
	}

	route, ok := cache.Select("orders", "session")
	if !ok || route.Addr != "bootstrap" {
		t.Fatalf("route = %+v, %v", route, ok)
	}
}

func TestRouteCacheAppliesRevisedEmptySnapshotRoutes(t *testing.T) {
	cache := frontend.NewRouteCache("bootstrap", []frontend.Route{{Role: "data-node", Addr: "bootstrap"}})
	updated := cache.UpdateFromSnapshot(controlapi.SnapshotBody{Revision: 8}, "control")
	if !updated {
		t.Fatal("expected revised empty snapshot to be applied")
	}

	if route, ok := cache.Select("orders", "session"); ok {
		t.Fatalf("unexpected route after empty snapshot: %+v", route)
	}
	snapshot := cache.Snapshot()
	if snapshot.RouteEpoch != 8 || len(snapshot.Routes) != 0 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}
