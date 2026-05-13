package control

import (
	"sort"
	"sync"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/internal/controlapi"
)

const (
	controlModeBootstrap = "bootstrap"
	nodeStateHealthy     = "healthy"
	nodeStateSuspect     = "suspect"
	nodeStateDead        = "dead"
)

type ControlState struct {
	mu        sync.RWMutex
	clusterID string
	revision  uint64
	nodes     *collectionmapping.Map[string, controlapi.NodeBody]
	now       func() time.Time
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
		clusterID: clusterID,
		nodes:     collectionmapping.NewMap[string, controlapi.NodeBody](),
		now:       now,
	}
}

func (s *ControlState) RegisterNode(nodeID, addr string) controlapi.RegisterNodeResponse {
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
	}
}

func (s *ControlState) Heartbeat(nodeID, addr string) controlapi.HeartbeatResponse {
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
	}
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

	routes := make([]controlapi.RouteBody, 0, len(nodes))
	for _, node := range nodes {
		if node.State != nodeStateHealthy {
			continue
		}
		routes = append(routes, controlapi.RouteBody{
			NodeID: node.NodeID,
			Addr:   node.Addr,
			Weight: 1,
		})
	}

	return controlapi.SnapshotBody{
		ClusterID: s.clusterID,
		Revision:  s.revision,
		Mode:      controlModeBootstrap,
		Nodes:     nodes,
		Routes:    routes,
	}
}

func (s *ControlState) sortedNodesLocked() []controlapi.NodeBody {
	nodes := s.nodes.Values()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return nodes
}
