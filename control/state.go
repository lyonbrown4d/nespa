package control

import (
	"context"
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
	eventBus   eventx.BusRuntime
	nextEvent  uint64
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
		eventBus:   bus,
		now:        now,
	}
}

func (s *ControlState) RegisterNode(ctx context.Context, nodeID, addr string) (controlapi.RegisterNodeResponse, error) {
	nodeID, addr, err := validateNodeIdentity(nodeID, addr)
	if err != nil {
		return controlapi.RegisterNodeResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	node := controlapi.NodeBody{
		NodeID:       nodeID,
		Addr:         addr,
		State:        nodeStateHealthy,
		LastSeenUnix: s.now().Unix(),
	}

	previous, exists := s.nodes.Get(nodeID)
	reason, changed := nodeRegistrationEventReason(previous, exists, node)
	if changed {
		s.revision++
	}
	s.nodes.Set(nodeID, node)
	if changed {
		s.recordRebalanceEventLocked(ctx, rebalanceEvent{
			reason: rebalanceReason(reason),
			node:   node,
		})
	}

	return controlapi.RegisterNodeResponse{
		Revision: s.revision,
		Node:     node,
	}, nil
}

func (s *ControlState) Heartbeat(ctx context.Context, nodeID, addr string) (controlapi.HeartbeatResponse, error) {
	nodeID, addr, err := validateNodeIdentity(nodeID, addr)
	if err != nil {
		return controlapi.HeartbeatResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes.Get(nodeID)
	var reason rebalanceReason
	changed := false
	if !exists {
		node = controlapi.NodeBody{
			NodeID: nodeID,
			Addr:   addr,
			State:  nodeStateHealthy,
		}
		s.revision++
		reason = rebalanceReasonNodeRegistered
		changed = true
	}
	if node.Addr != addr {
		node.Addr = addr
		s.revision++
		reason = rebalanceReasonNodeAddress
		changed = true
	}
	if node.State != nodeStateHealthy {
		node.State = nodeStateHealthy
		s.revision++
		reason = rebalanceReasonNodeRecovered
		changed = true
	}
	node.LastSeenUnix = s.now().Unix()
	s.nodes.Set(nodeID, node)
	if changed {
		s.recordRebalanceEventLocked(ctx, rebalanceEvent{
			reason: reason,
			node:   node,
		})
	}

	return controlapi.HeartbeatResponse{
		Revision: s.revision,
		Node:     node,
	}, nil
}

func (s *ControlState) AdvanceLiveness(ctx context.Context, now time.Time, suspectAfter, deadAfter time.Duration) LivenessResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	var changed []controlapi.NodeBody
	s.nodes.Range(func(nodeID string, node controlapi.NodeBody) bool {
		if node.LastSeenUnix <= 0 {
			return true
		}

		nextState := node.State
		lastSeen := time.Unix(node.LastSeenUnix, 0)
		age := now.Sub(lastSeen)
		switch {
		case deadAfter > 0 && age >= deadAfter:
			nextState = nodeStateDead
		case suspectAfter > 0 && age >= suspectAfter:
			nextState = nodeStateSuspect
		}

		if node.State == nextState {
			return true
		}
		node.State = nextState
		s.nodes.Set(nodeID, node)
		s.revision++
		changed = append(changed, node)
		s.recordRebalanceEventLocked(ctx, rebalanceEvent{
			reason: livenessRebalanceReason(nextState),
			node:   node,
		})
		return true
	})

	return LivenessResult{
		Revision: s.revision,
		Changed:  changed,
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
