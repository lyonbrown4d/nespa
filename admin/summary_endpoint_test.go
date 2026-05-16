package admin_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
)

type fakeSummaryCacheService struct {
	stats cache.Stats
	err   error
}

func (f fakeSummaryCacheService) Stats(context.Context) (cache.Stats, error) {
	return f.stats, f.err
}

type fakeSummaryControlService struct {
	namespaces controlapi.NamespacesBody
	spaces     controlapi.SpacesBody
	nodes      controlapi.NodesBody
	revision   uint64
	routeCount uint64
}

func (f fakeSummaryControlService) Namespaces() controlapi.NamespacesBody { return f.namespaces }
func (f fakeSummaryControlService) Spaces() controlapi.SpacesBody         { return f.spaces }
func (f fakeSummaryControlService) Nodes() controlapi.NodesBody           { return f.nodes }
func (f fakeSummaryControlService) Revision() uint64                      { return f.revision }
func (f fakeSummaryControlService) RouteCount() uint64                    { return f.routeCount }

type fakeSummaryNodeService struct {
	routeEpoch uint64
}

func (f fakeSummaryNodeService) RouteEpoch() uint64 {
	return f.routeEpoch
}

type summaryEndpointForTest interface {
	Summary(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[admin.SummaryBody], error)
}

func TestSummaryReturnsRuntimeStats(t *testing.T) {
	endpoint := newSummaryEndpointForTest(t)

	got, err := endpoint.Summary(context.Background(), &runtime.EmptyInput{})
	if err != nil {
		t.Fatalf("Summary() error = %v", err)
	}
	if got == nil {
		t.Fatal("Summary() returned nil response")
	}

	assertSummaryClusterStats(t, got.Body)
	assertSummaryCacheStats(t, got.Body)
}

func newSummaryEndpointForTest(t *testing.T) summaryEndpointForTest {
	t.Helper()

	cachedSvc := fakeSummaryCacheService{
		stats: cache.Stats{
			Objects:       3,
			MemoryBytes:   128,
			GetRequests:   10,
			GetHits:       7,
			GetMisses:     2,
			GetExpired:    1,
			TouchRequests: 4,
			TouchHits:     3,
			TouchMisses:   1,
			Evictions:     2,
		},
	}

	controlSvc := fakeSummaryControlService{
		namespaces: controlapi.NamespacesBody{
			Namespaces: []controlapi.NamespaceBody{{Namespace: "orders"}, {Namespace: "billing"}},
		},
		spaces: controlapi.SpacesBody{
			Spaces: []controlapi.SpaceBody{{Namespace: "orders", Space: "session"}, {Namespace: "orders", Space: "view"}},
		},
		nodes: controlapi.NodesBody{
			Nodes: []controlapi.NodeBody{{NodeID: "node-1"}, {NodeID: "node-2"}},
		},
		revision:   42,
		routeCount: 2,
	}
	nodeSvc := fakeSummaryNodeService{
		routeEpoch: 7,
	}

	endpoint, ok := admin.NewSummaryEndpoint(admin.Config{ControlAddr: "127.0.0.1:7401"}, cachedSvc, controlSvc, nodeSvc).(summaryEndpointForTest)
	if !ok {
		t.Fatal("summary endpoint does not expose Summary")
	}
	return endpoint
}

func assertSummaryClusterStats(t *testing.T, body admin.SummaryBody) {
	t.Helper()

	if body.ControlAddr != "127.0.0.1:7401" {
		t.Fatalf("control_addr = %q, want 127.0.0.1:7401", body.ControlAddr)
	}
	if body.Namespaces != 2 || body.Spaces != 2 || body.Nodes != 2 {
		t.Fatalf("counts = %+v", body)
	}
	if body.ControlRevision != 42 {
		t.Fatalf("control_revision = %d, want 42", body.ControlRevision)
	}
	if body.RouteCount != 2 {
		t.Fatalf("route_count = %d, want 2", body.RouteCount)
	}
	if body.NodeRouteEpoch != 7 {
		t.Fatalf("node_route_epoch = %d, want 7", body.NodeRouteEpoch)
	}
}

func assertSummaryCacheStats(t *testing.T, body admin.SummaryBody) {
	t.Helper()

	if body.CacheObjects != 3 || body.CacheMemory != 128 {
		t.Fatalf("cache stats = %+v", body)
	}
	if body.CacheGetRequests != 10 || body.CacheTouchMisses != 1 {
		t.Fatalf("cache IO stats = %+v", body)
	}
}
