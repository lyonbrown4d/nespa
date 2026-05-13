package control

import (
	"sort"
	"sync"
	"time"

	"github.com/lyonbrown4d/nespa/internal/controlapi"
)

const (
	controlModeBootstrap = "bootstrap"
	nodeStateHealthy     = "healthy"
)

type ControlState struct {
	mu        sync.RWMutex
	clusterID string
	revision  uint64
	nodes     map[string]controlapi.NodeBody
	now       func() time.Time
}

func NewControlState(clusterID string) *ControlState {
	return &ControlState{
		clusterID: clusterID,
		nodes:     make(map[string]controlapi.NodeBody),
		now:       time.Now,
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

	previous, exists := s.nodes[nodeID]
	if !exists || previous.Addr != addr || previous.State != node.State {
		s.revision++
	}
	s.nodes[nodeID] = node

	return controlapi.RegisterNodeResponse{
		Revision: s.revision,
		Node:     node,
	}
}

func (s *ControlState) Heartbeat(nodeID, addr string) controlapi.HeartbeatResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[nodeID]
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
	s.nodes[nodeID] = node

	return controlapi.HeartbeatResponse{
		Revision: s.revision,
		Node:     node,
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

func (s *ControlState) Snapshot() controlapi.SnapshotBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]controlapi.NodeBody, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})

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
