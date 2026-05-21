package admin_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
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

type fakeSummaryReplicationService struct {
	stats cachetcp.ReplicationStats
}

func (f fakeSummaryReplicationService) ReplicationStats() cachetcp.ReplicationStats {
	return f.stats
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
	assertSummaryReplicationStats(t, got.Body)
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
	replicationSvc := fakeSummaryReplicationService{
		stats: cachetcp.ReplicationStats{
			QueueDepth:          2,
			QueueCapacity:       16,
			Enqueued:            5,
			Dropped:             1,
			Attempts:            7,
			Successes:           4,
			Failures:            3,
			OutboxEntries:       5,
			OutboxFailures:      1,
			AckTargets:          2,
			AckFailures:         1,
			LastQueuedSequence:  8,
			LastAttemptSequence: 7,
			LastSuccessSequence: 6,
			LastFailureSequence: 7,
			LastDroppedSequence: 3,
			LastOutboxSequence:  8,
			LastAckSequence:     6,
			Retrying:            true,
			ActiveTarget:        "127.0.0.1:7503",
			LastAckTarget:       "127.0.0.1:7504",
			LastAckError:        "ack write failed",
			LastOutboxError:     "sync failed",
			LastError:           "dial failed",
			LastErrorUnixMs:     1000,
			LastSuccessUnixMs:   900,
		},
	}

	endpoint, ok := admin.NewSummaryEndpoint(
		admin.Config{ControlAddr: "127.0.0.1:7401"},
		cachedSvc,
		controlSvc,
		nodeSvc,
		replicationSvc,
	).(summaryEndpointForTest)
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

func assertSummaryReplicationStats(t *testing.T, body admin.SummaryBody) {
	t.Helper()
	assertSummaryReplicationQueueStats(t, body.Replication)
	assertSummaryReplicationSendStats(t, body.Replication)
	assertSummaryReplicationOutboxStats(t, body.Replication)
	assertSummaryReplicationAckStats(t, body.Replication)
	assertSummaryReplicationSequenceStats(t, body.Replication)
	assertSummaryReplicationRetryStats(t, body.Replication)
}

func assertSummaryReplicationQueueStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if body.QueueDepth != 2 || body.QueueCapacity != 16 {
		t.Fatalf("replication queue stats = %+v", body)
	}
	if body.Enqueued != 5 || body.Dropped != 1 {
		t.Fatalf("replication enqueue stats = %+v", body)
	}
}

func assertSummaryReplicationSendStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if body.Attempts != 7 || body.Successes != 4 || body.Failures != 3 {
		t.Fatalf("replication send stats = %+v", body)
	}
}

func assertSummaryReplicationOutboxStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if body.OutboxEntries != 5 || body.OutboxFailures != 1 || body.LastOutboxSequence != 8 {
		t.Fatalf("replication outbox stats = %+v", body)
	}
	if body.LastOutboxError != "sync failed" {
		t.Fatalf("replication outbox error = %+v", body)
	}
}

func assertSummaryReplicationAckStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if body.AckTargets != 2 || body.AckFailures != 1 || body.LastAckSequence != 6 {
		t.Fatalf("replication ack stats = %+v", body)
	}
	if body.LastAckTarget != "127.0.0.1:7504" || body.LastAckError != "ack write failed" {
		t.Fatalf("replication ack detail = %+v", body)
	}
}

func assertSummaryReplicationSequenceStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if body.LastQueuedSequence != 8 || body.LastAttemptSequence != 7 {
		t.Fatalf("replication active sequence stats = %+v", body)
	}
	if body.LastSuccessSequence != 6 || body.LastFailureSequence != 7 || body.LastDroppedSequence != 3 {
		t.Fatalf("replication terminal sequence stats = %+v", body)
	}
}

func assertSummaryReplicationRetryStats(t *testing.T, body admin.ReplicationBody) {
	t.Helper()
	if !body.Retrying || body.ActiveTarget != "127.0.0.1:7503" {
		t.Fatalf("replication retry stats = %+v", body)
	}
	if body.LastError != "dial failed" || body.LastErrorUnixMs != 1000 {
		t.Fatalf("replication last error stats = %+v", body)
	}
}
