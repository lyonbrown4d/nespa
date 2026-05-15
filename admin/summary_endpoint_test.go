package admin

import (
	"context"
	"testing"

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
}

func (f fakeSummaryControlService) Namespaces() controlapi.NamespacesBody { return f.namespaces }
func (f fakeSummaryControlService) Spaces() controlapi.SpacesBody         { return f.spaces }
func (f fakeSummaryControlService) Nodes() controlapi.NodesBody           { return f.nodes }

func TestSummaryReturnsRuntimeStats(t *testing.T) {
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
	}

	endpoint := &summaryEndpoint{
		cfg:        Config{ControlAddr: "127.0.0.1:7401"},
		cacheSvc:   cachedSvc,
		controlSvc: controlSvc,
	}

	got, err := endpoint.Summary(context.Background(), &runtime.EmptyInput{})
	if err != nil {
		t.Fatalf("Summary() error = %v", err)
	}
	if got == nil {
		t.Fatal("Summary() returned nil response")
	}

	body := got.Body
	if body.ControlAddr != "127.0.0.1:7401" {
		t.Fatalf("control_addr = %q, want 127.0.0.1:7401", body.ControlAddr)
	}
	if body.Namespaces != 2 || body.Spaces != 2 || body.Nodes != 2 {
		t.Fatalf("counts = %+v", body)
	}
	if body.CacheObjects != 3 || body.CacheMemory != 128 {
		t.Fatalf("cache stats = %+v", body)
	}
	if body.CacheGetRequests != 10 || body.CacheTouchMisses != 1 {
		t.Fatalf("cache IO stats = %+v", body)
	}
}
