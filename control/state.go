package control

import (
	"sync"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/eventx"
	"github.com/lyonbrown4d/nespa/controlapi"
)

const (
	controlModeBootstrap = "bootstrap"
	nodeStateHealthy     = "healthy"
	nodeStateSuspect     = "suspect"
	nodeStateDead        = "dead"

	rebalanceEventRouteTableChanged = "route_table_changed"
	rebalanceReasonNodeRegistered   = "node_registered"
	rebalanceReasonNodeAddress      = "node_address_changed"
	rebalanceReasonNodeRecovered    = "node_recovered"
	rebalanceReasonNodeSuspect      = "node_suspect"
	rebalanceReasonNodeDead         = "node_dead"
	rebalanceReasonSpaceCreated     = "space_created"

	maxRebalanceEvents = 128
	maxMigrationPlans  = 128
)

const RebalanceEventName = "control.rebalance"

type RebalanceEvent struct {
	Event controlapi.RebalanceEventBody
}

func (RebalanceEvent) Name() string {
	return RebalanceEventName
}

type ControlState struct {
	mu         sync.RWMutex
	clusterID  string
	revision   uint64
	namespaces *collectionmapping.Map[string, controlapi.NamespaceBody]
	spaces     *collectionmapping.Map[spaceRef, controlapi.SpaceBody]
	entities   *collectionmapping.Map[entityRef, controlapi.EntityBody]
	nodes      *collectionmapping.Map[string, controlapi.NodeBody]
	events     *collectionlist.List[controlapi.RebalanceEventBody]
	plans      *collectionlist.List[controlapi.MigrationPlanBody]
	eventBus   eventx.BusRuntime
	nextEvent  uint64
	nextPlan   uint64
	lastRoutes []controlapi.RouteBody
	now        func() time.Time
}

type LivenessResult struct {
	Revision uint64
	Changed  []controlapi.NodeBody
}

func NewControlState(clusterID string) *ControlState {
	return NewControlStateWithClock(clusterID, time.Now)
}

func NewControlStateWithClock(clusterID string, now func() time.Time) *ControlState {
	return NewControlStateWithClockAndEvents(clusterID, now, nil)
}

func NewControlStateWithEvents(clusterID string, bus eventx.BusRuntime) *ControlState {
	return NewControlStateWithClockAndEvents(clusterID, time.Now, bus)
}

func NewControlStateWithClockAndEvents(clusterID string, now func() time.Time, bus eventx.BusRuntime) *ControlState {
	if now == nil {
		now = time.Now
	}
	return &ControlState{
		clusterID:  clusterID,
		namespaces: collectionmapping.NewMap[string, controlapi.NamespaceBody](),
		spaces:     collectionmapping.NewMap[spaceRef, controlapi.SpaceBody](),
		entities:   collectionmapping.NewMap[entityRef, controlapi.EntityBody](),
		nodes:      collectionmapping.NewMap[string, controlapi.NodeBody](),
		events:     collectionlist.NewList[controlapi.RebalanceEventBody](),
		plans:      collectionlist.NewList[controlapi.MigrationPlanBody](),
		eventBus:   bus,
		now:        now,
	}
}

func (s *ControlState) State() controlapi.StateBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.StateBody{
		ClusterID: s.clusterID,
		Revision:  s.revision,
		Mode:      controlModeBootstrap,
	}
}

func (s *ControlState) Nodes() controlapi.NodesBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.NodesBody{
		Revision: s.revision,
		Nodes:    s.sortedNodesLocked(),
	}
}

func (s *ControlState) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.revision
}

func (s *ControlState) RouteCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := s.sortedNodesLocked()
	spaces := s.sortedSpacesLocked()
	return len(routesForNodes(nodes, spaces))
}

func (s *ControlState) RebalanceEvents() controlapi.RebalanceEventsBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.RebalanceEventsBody{
		Revision: s.revision,
		Events:   s.events.Values(),
	}
}

func (s *ControlState) MigrationPlans() controlapi.MigrationPlansBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.MigrationPlansBody{
		Revision: s.revision,
		Plans:    s.plans.Values(),
	}
}

func (s *ControlState) Snapshot() controlapi.SnapshotBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := s.sortedNodesLocked()
	namespaces := s.sortedNamespacesLocked()
	spaces := s.sortedSpacesLocked()
	entities := s.sortedEntitiesLocked()

	return controlapi.SnapshotBody{
		ClusterID:  s.clusterID,
		Revision:   s.revision,
		Mode:       controlModeBootstrap,
		Namespaces: namespaces,
		Spaces:     spaces,
		Entities:   entities,
		Nodes:      nodes,
		Routes:     routesForNodes(nodes, spaces),
	}
}
