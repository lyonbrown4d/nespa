package control

import (
	"sort"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func (s *ControlState) effectiveRoutesLocked() []controlapi.RouteBody {
	routes := routesForNodes(s.sortedNodesLocked(), s.sortedSpacesLocked())
	return applyMigrationRouteOverrides(routes, s.plans.Values())
}

func applyMigrationRouteOverrides(
	routes []controlapi.RouteBody,
	plans []controlapi.MigrationPlanBody,
) []controlapi.RouteBody {
	out := append([]controlapi.RouteBody(nil), routes...)
	for planIndex := range plans {
		for taskIndex := range plans[planIndex].Tasks {
			task := plans[planIndex].Tasks[taskIndex]
			if migrationTaskRoutesSource(task) {
				out = overrideRouteRange(out, task)
			}
		}
	}
	sortRoutes(out)
	return out
}

func migrationTaskRoutesSource(task controlapi.MigrationTaskBody) bool {
	return task.CutoverAtUnix == 0 && task.State != migrationTaskDone
}

func overrideRouteRange(
	routes []controlapi.RouteBody,
	task controlapi.MigrationTaskBody,
) []controlapi.RouteBody {
	out := make([]controlapi.RouteBody, 0, len(routes)+2)
	for index := range routes {
		route := routes[index]
		start, end, ok := taskRouteOverlap(route, task)
		if !ok {
			out = append(out, route)
			continue
		}
		if route.VSlotStart < start {
			out = appendRouteSegment(out, route, route.VSlotStart, start-1)
		}
		out = appendRouteSegment(out, sourceRouteForTask(task), start, end)
		if end < route.VSlotEnd {
			out = appendRouteSegment(out, route, end+1, route.VSlotEnd)
		}
	}
	return out
}

func taskRouteOverlap(route controlapi.RouteBody, task controlapi.MigrationTaskBody) (uint32, uint32, bool) {
	if route.Namespace != task.Namespace || route.Space != task.Space {
		return 0, 0, false
	}
	start := maxUint32(route.VSlotStart, task.VSlotStart)
	end := minUint32(route.VSlotEnd, task.VSlotEnd)
	return start, end, start <= end
}

func sourceRouteForTask(task controlapi.MigrationTaskBody) controlapi.RouteBody {
	return controlapi.RouteBody{
		Namespace:  task.Namespace,
		Space:      task.Space,
		NodeID:     task.SourceNodeID,
		Addr:       task.SourceAddr,
		Weight:     1,
	}
}

func appendRouteSegment(
	routes []controlapi.RouteBody,
	route controlapi.RouteBody,
	start, end uint32,
) []controlapi.RouteBody {
	if start > end {
		return routes
	}
	route.VSlotStart = start
	route.VSlotEnd = end
	return append(routes, route)
}

func sortRoutes(routes []controlapi.RouteBody) {
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Namespace != routes[j].Namespace {
			return routes[i].Namespace < routes[j].Namespace
		}
		if routes[i].Space != routes[j].Space {
			return routes[i].Space < routes[j].Space
		}
		return routes[i].VSlotStart < routes[j].VSlotStart
	})
}
