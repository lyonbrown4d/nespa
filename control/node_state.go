package control

import (
	"context"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func (s *ControlState) RegisterNode(ctx context.Context, nodeID, addr string) (controlapi.RegisterNodeResponse, error) {
	return s.registerNodeAt(ctx, nodeID, addr, s.now())
}

func (s *ControlState) registerNodeAt(ctx context.Context, nodeID, addr string, now time.Time) (controlapi.RegisterNodeResponse, error) {
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
		LastSeenUnix: now.Unix(),
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
	return s.heartbeatAt(ctx, nodeID, addr, s.now())
}

func (s *ControlState) heartbeatAt(ctx context.Context, nodeID, addr string, now time.Time) (controlapi.HeartbeatResponse, error) {
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
	node.LastSeenUnix = now.Unix()
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
