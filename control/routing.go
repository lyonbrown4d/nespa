package control

import (
	"sort"

	"github.com/lyonbrown4d/nespa/controlapi"
)

const defaultRouteReplicaCount = 1

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
			Replicas:   routeReplicas(healthy, index, defaultRouteReplicaCount),
		})
	}
	return routes
}

func routeReplicas(nodes []controlapi.NodeBody, primaryIndex, replicaCount int) []controlapi.RouteReplicaBody {
	limit := routeReplicaLimit(len(nodes), replicaCount)
	if limit == 0 {
		return nil
	}

	replicas := make([]controlapi.RouteReplicaBody, 0, limit)
	for offset := 1; offset <= limit; offset++ {
		node := nodes[(primaryIndex+offset)%len(nodes)]
		replicas = append(replicas, controlapi.RouteReplicaBody{
			NodeID: node.NodeID,
			Addr:   node.Addr,
			Weight: 1,
		})
	}
	return replicas
}

func routeReplicaLimit(nodeCount, replicaCount int) int {
	if nodeCount <= 1 || replicaCount <= 0 {
		return 0
	}
	if replicaCount >= nodeCount {
		return nodeCount - 1
	}
	return replicaCount
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
