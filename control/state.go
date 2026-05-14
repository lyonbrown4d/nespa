package control

import (
	"sort"
	"sync"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/controlapi"
)

const (
	controlModeBootstrap = "bootstrap"
	nodeStateHealthy     = "healthy"
	nodeStateSuspect     = "suspect"
	nodeStateDead        = "dead"
)

type ControlState struct {
	mu         sync.RWMutex
	clusterID  string
	revision   uint64
	namespaces *collectionmapping.Map[string, controlapi.NamespaceBody]
	spaces     *collectionmapping.Map[spaceRef, controlapi.SpaceBody]
	nodes      *collectionmapping.Map[string, controlapi.NodeBody]
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
	if now == nil {
		now = time.Now
	}
	return &ControlState{
		clusterID:  clusterID,
		namespaces: collectionmapping.NewMap[string, controlapi.NamespaceBody](),
		spaces:     collectionmapping.NewMap[spaceRef, controlapi.SpaceBody](),
		nodes:      collectionmapping.NewMap[string, controlapi.NodeBody](),
		now:        now,
	}
}

func (s *ControlState) RegisterNode(nodeID, addr string) (controlapi.RegisterNodeResponse, error) {
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
	if !exists || previous.Addr != addr || previous.State != node.State {
		s.revision++
	}
	s.nodes.Set(nodeID, node)

	return controlapi.RegisterNodeResponse{
		Revision: s.revision,
		Node:     node,
	}, nil
}

func (s *ControlState) Heartbeat(nodeID, addr string) (controlapi.HeartbeatResponse, error) {
	nodeID, addr, err := validateNodeIdentity(nodeID, addr)
	if err != nil {
		return controlapi.HeartbeatResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes.Get(nodeID)
	if !exists {
		node = controlapi.NodeBody{
			NodeID: nodeID,
			Addr:   addr,
			State:  nodeStateHealthy,
		}
		s.revision++
	}
	if node.Addr != addr {
		node.Addr = addr
		s.revision++
	}
	if node.State != nodeStateHealthy {
		node.State = nodeStateHealthy
		s.revision++
	}
	node.LastSeenUnix = s.now().Unix()
	s.nodes.Set(nodeID, node)

	return controlapi.HeartbeatResponse{
		Revision: s.revision,
		Node:     node,
	}, nil
}

func (s *ControlState) AdvanceLiveness(now time.Time, suspectAfter, deadAfter time.Duration) LivenessResult {
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

func (s *ControlState) Snapshot() controlapi.SnapshotBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := s.sortedNodesLocked()
	namespaces := s.sortedNamespacesLocked()
	spaces := s.sortedSpacesLocked()

	return controlapi.SnapshotBody{
		ClusterID:  s.clusterID,
		Revision:   s.revision,
		Mode:       controlModeBootstrap,
		Namespaces: namespaces,
		Spaces:     spaces,
		Nodes:      nodes,
		Routes:     routesForNodes(nodes, spaces),
	}
}

func (s *ControlState) sortedNodesLocked() []controlapi.NodeBody {
	nodes := s.nodes.Values()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return nodes
}

func routesForNodes(nodes []controlapi.NodeBody, spaces []controlapi.SpaceBody) []controlapi.RouteBody {
	healthy := healthyNodes(nodes)
	if len(healthy) == 0 {
		return nil
	}
	if len(spaces) == 0 {
		return routesForSpace(healthy, controlapi.SpaceBody{})
	}

	routes := make([]controlapi.RouteBody, 0, len(healthy)*len(spaces))
	for _, space := range spaces {
		routes = append(routes, routesForSpace(healthy, space)...)
	}
	return routes
}

func routesForSpace(healthy []controlapi.NodeBody, space controlapi.SpaceBody) []controlapi.RouteBody {
	routes := make([]controlapi.RouteBody, 0, len(healthy))
	for index, node := range healthy {
		start, end := vslotRange(index, len(healthy))
		routes = append(routes, controlapi.RouteBody{
			Namespace:  space.Namespace,
			Space:      space.Space,
			VSlotStart: start,
			VSlotEnd:   end,
			NodeID:     node.NodeID,
			Addr:       node.Addr,
			Weight:     1,
		})
	}
	return routes
}

func healthyNodes(nodes []controlapi.NodeBody) []controlapi.NodeBody {
	healthy := make([]controlapi.NodeBody, 0, len(nodes))
	for _, node := range nodes {
		if node.State == nodeStateHealthy {
			healthy = append(healthy, node)
		}
	}
	return healthy
}

func vslotRange(index, count int) (uint32, uint32) {
	start := checkedUint64(index) * uint64(controlapi.VSlotCount) / checkedUint64(count)
	end := (checkedUint64(index+1)*uint64(controlapi.VSlotCount))/checkedUint64(count) - 1
	return checkedVSlot(start), checkedVSlot(end)
}

func checkedUint64(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func checkedVSlot(value uint64) uint32 {
	if value > uint64(controlapi.VSlotMax) {
		return controlapi.VSlotMax
	}
	return uint32(value)
}
