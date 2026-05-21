package admin

import (
	"context"
	"fmt"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

type Endpoint interface {
	httpx.Endpoint
	adminEndpoint()
}

type summaryCacheProvider interface {
	Stats(context.Context) (cache.Stats, error)
}

type summaryControlProvider interface {
	Namespaces() controlapi.NamespacesBody
	Spaces() controlapi.SpacesBody
	Nodes() controlapi.NodesBody
	Revision() uint64
	RouteCount() uint64
}

type summaryNodeProvider interface {
	RouteEpoch() uint64
}

type summaryReplicationProvider interface {
	ReplicationStats() cachetcp.ReplicationStats
}

type summaryEndpoint struct {
	cfg            Config
	cacheSvc       summaryCacheProvider
	controlSvc     summaryControlProvider
	nodeSvc        summaryNodeProvider
	replicationSvc summaryReplicationProvider
}

func NewSummaryEndpoint(
	cfg Config,
	cacheSvc summaryCacheProvider,
	controlSvc summaryControlProvider,
	nodeSvc summaryNodeProvider,
	replicationSvc summaryReplicationProvider,
) Endpoint {
	return &summaryEndpoint{
		cfg:            cfg,
		cacheSvc:       cacheSvc,
		controlSvc:     controlSvc,
		nodeSvc:        nodeSvc,
		replicationSvc: replicationSvc,
	}
}

func (e *summaryEndpoint) adminEndpoint() {}

func (e *summaryEndpoint) EndpointSpec() httpx.EndpointSpec {
	return httpx.EndpointSpec{
		Prefix: "/v1/admin",
	}
}

func (e *summaryEndpoint) Register(registrar httpx.Registrar) {
	httpx.MustGroupGet(registrar.Scope(), "/summary", e.Summary)
}

func (e *summaryEndpoint) Summary(ctx context.Context, _ *runtime.EmptyInput) (*runtime.JSONResponse[SummaryBody], error) {
	stats, err := e.cacheSvc.Stats(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cache stats: %w", err)
	}

	namespaces := e.controlSvc.Namespaces()
	spaces := e.controlSvc.Spaces()
	nodes := e.controlSvc.Nodes()
	nodeRouteEpoch := uint64(0)
	if e.nodeSvc != nil {
		nodeRouteEpoch = e.nodeSvc.RouteEpoch()
	}
	replication := ReplicationBody{}
	if e.replicationSvc != nil {
		replication = replicationBodyFromStats(e.replicationSvc.ReplicationStats())
	}

	return runtime.JSON(SummaryBody{
		ControlAddr:        e.cfg.ControlAddr,
		Namespaces:         uint64(len(namespaces.Namespaces)),
		Spaces:             uint64(len(spaces.Spaces)),
		Nodes:              uint64(len(nodes.Nodes)),
		ControlRevision:    e.controlSvc.Revision(),
		RouteCount:         e.controlSvc.RouteCount(),
		NodeRouteEpoch:     nodeRouteEpoch,
		CacheMemory:        stats.MemoryBytes,
		CacheObjects:       stats.Objects,
		CacheGetRequests:   stats.GetRequests,
		CacheGetHits:       stats.GetHits,
		CacheGetMisses:     stats.GetMisses,
		CacheGetExpired:    stats.GetExpired,
		CacheTouchRequests: stats.TouchRequests,
		CacheTouchHits:     stats.TouchHits,
		CacheTouchMisses:   stats.TouchMisses,
		CacheEvictions:     stats.Evictions,
		Replication:        replication,
	}), nil
}

func replicationBodyFromStats(stats cachetcp.ReplicationStats) ReplicationBody {
	return ReplicationBody{
		QueueDepth:          stats.QueueDepth,
		QueueCapacity:       stats.QueueCapacity,
		Enqueued:            stats.Enqueued,
		Dropped:             stats.Dropped,
		Attempts:            stats.Attempts,
		Successes:           stats.Successes,
		Failures:            stats.Failures,
		OutboxEntries:       stats.OutboxEntries,
		OutboxFailures:      stats.OutboxFailures,
		AckTargets:          stats.AckTargets,
		AckFailures:         stats.AckFailures,
		LastQueuedSequence:  stats.LastQueuedSequence,
		LastAttemptSequence: stats.LastAttemptSequence,
		LastSuccessSequence: stats.LastSuccessSequence,
		LastFailureSequence: stats.LastFailureSequence,
		LastDroppedSequence: stats.LastDroppedSequence,
		LastOutboxSequence:  stats.LastOutboxSequence,
		LastAckSequence:     stats.LastAckSequence,
		Retrying:            stats.Retrying,
		ActiveTarget:        stats.ActiveTarget,
		LastAckTarget:       stats.LastAckTarget,
		LastAckError:        stats.LastAckError,
		LastOutboxError:     stats.LastOutboxError,
		LastError:           stats.LastError,
		LastErrorUnixMs:     stats.LastErrorUnixMs,
		LastSuccessUnixMs:   stats.LastSuccessUnixMs,
	}
}
