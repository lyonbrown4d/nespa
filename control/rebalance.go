package control

import (
	"context"

	"github.com/lyonbrown4d/nespa/controlapi"
)

type rebalanceReason string

type rebalanceEvent struct {
	reason    rebalanceReason
	node      controlapi.NodeBody
	namespace string
	space     string
}

func nodeRegistrationEventReason(previous controlapi.NodeBody, exists bool, next controlapi.NodeBody) (string, bool) {
	switch {
	case !exists:
		return rebalanceReasonNodeRegistered, true
	case previous.Addr != next.Addr:
		return rebalanceReasonNodeAddress, true
	case previous.State != next.State:
		return rebalanceReasonNodeRecovered, true
	default:
		return "", false
	}
}

func livenessRebalanceReason(state string) rebalanceReason {
	switch state {
	case nodeStateSuspect:
		return rebalanceReasonNodeSuspect
	case nodeStateDead:
		return rebalanceReasonNodeDead
	default:
		return rebalanceReasonNodeRecovered
	}
}

func (s *ControlState) recordRebalanceEventLocked(ctx context.Context, event rebalanceEvent) {
	if event.reason == "" {
		return
	}
	s.nextEvent++
	body := controlapi.RebalanceEventBody{
		ID:            s.nextEvent,
		Revision:      s.revision,
		Type:          rebalanceEventRouteTableChanged,
		Reason:        string(event.reason),
		NodeID:        event.node.NodeID,
		Addr:          event.node.Addr,
		State:         event.node.State,
		Namespace:     event.namespace,
		Space:         event.space,
		RouteCount:    len(routesForNodes(s.sortedNodesLocked(), s.sortedSpacesLocked())),
		CreatedAtUnix: s.now().Unix(),
	}
	s.events.Add(body)
	for s.events.Len() > maxRebalanceEvents {
		s.events.RemoveAt(0)
	}
	if s.eventBus != nil {
		if err := s.eventBus.PublishAsync(ctx, RebalanceEvent{Event: body}); err != nil {
			return
		}
	}
}
