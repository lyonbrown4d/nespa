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
	previousRoutes := append([]controlapi.RouteBody(nil), s.lastRoutes...)
	currentRoutes := routesForNodes(s.sortedNodesLocked(), s.sortedSpacesLocked())
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
		RouteCount:    len(currentRoutes),
		CreatedAtUnix: s.now().Unix(),
	}
	s.events.Add(body)
	for s.events.Len() > maxRebalanceEvents {
		s.events.RemoveAt(0)
	}
	s.recordMigrationPlanLocked(body, previousRoutes, currentRoutes)
	s.lastRoutes = currentRoutes
	if s.eventBus != nil {
		if err := s.eventBus.PublishAsync(ctx, RebalanceEvent{Event: body}); err != nil {
			return
		}
	}
}

func (s *ControlState) recordMigrationPlanLocked(
	event controlapi.RebalanceEventBody,
	previousRoutes []controlapi.RouteBody,
	currentRoutes []controlapi.RouteBody,
) {
	tasks := buildMigrationTasks(event, previousRoutes, currentRoutes)
	if len(tasks) == 0 {
		return
	}
	s.nextPlan++
	for index := range tasks {
		tasks[index].PlanID = s.nextPlan
		tasks[index].TaskID = uint64(index + 1)
	}
	plan := controlapi.MigrationPlanBody{
		ID:            s.nextPlan,
		Revision:      event.Revision,
		Reason:        event.Reason,
		State:         "planned",
		CreatedAtUnix: event.CreatedAtUnix,
		Tasks:         tasks,
	}
	s.plans.Add(plan)
	for s.plans.Len() > maxMigrationPlans {
		s.plans.RemoveAt(0)
	}
}

func buildMigrationTasks(
	event controlapi.RebalanceEventBody,
	previousRoutes []controlapi.RouteBody,
	currentRoutes []controlapi.RouteBody,
) []controlapi.MigrationTaskBody {
	tasks := make([]controlapi.MigrationTaskBody, 0, len(previousRoutes))
	for currentIndex := range currentRoutes {
		tasks = append(tasks, migrationTasksForRoute(event, previousRoutes, currentRoutes[currentIndex])...)
	}
	return tasks
}

func migrationTasksForRoute(
	event controlapi.RebalanceEventBody,
	previousRoutes []controlapi.RouteBody,
	current controlapi.RouteBody,
) []controlapi.MigrationTaskBody {
	if emptyRouteScope(current) {
		return nil
	}

	tasks := make([]controlapi.MigrationTaskBody, 0)
	for previousIndex := range previousRoutes {
		previous := previousRoutes[previousIndex]
		start, end, ok := migrationTaskRange(previous, current)
		if !ok {
			continue
		}
		tasks = append(tasks, migrationTaskForRoute(event, previous, current, start, end))
	}
	return tasks
}

func migrationTaskRange(previous, current controlapi.RouteBody) (uint32, uint32, bool) {
	if emptyRouteScope(previous) || !sameRouteScope(previous, current) || previous.NodeID == current.NodeID {
		return 0, 0, false
	}
	return routeOverlap(previous, current)
}

func migrationTaskForRoute(
	event controlapi.RebalanceEventBody,
	previous, current controlapi.RouteBody,
	start, end uint32,
) controlapi.MigrationTaskBody {
	return controlapi.MigrationTaskBody{
		Revision:      event.Revision,
		Namespace:     current.Namespace,
		Space:         current.Space,
		VSlotStart:    start,
		VSlotEnd:      end,
		SourceNodeID:  previous.NodeID,
		SourceAddr:    previous.Addr,
		TargetNodeID:  current.NodeID,
		TargetAddr:    current.Addr,
		State:         "planned",
		CreatedAtUnix: event.CreatedAtUnix,
	}
}

func sameRouteScope(left, right controlapi.RouteBody) bool {
	return left.Namespace == right.Namespace && left.Space == right.Space
}

func emptyRouteScope(route controlapi.RouteBody) bool {
	return route.Namespace == "" && route.Space == ""
}

func routeOverlap(left, right controlapi.RouteBody) (uint32, uint32, bool) {
	start := maxUint32(left.VSlotStart, right.VSlotStart)
	end := minUint32(left.VSlotEnd, right.VSlotEnd)
	return start, end, start <= end
}

func minUint32(left, right uint32) uint32 {
	if left < right {
		return left
	}
	return right
}

func maxUint32(left, right uint32) uint32 {
	if left > right {
		return left
	}
	return right
}
