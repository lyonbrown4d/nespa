package frontend

import (
	"sync"

	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
)

type Route struct {
	Namespace  string         `json:"namespace,omitempty"`
	Space      string         `json:"space,omitempty"`
	VSlotStart uint32         `json:"vslot_start"`
	VSlotEnd   uint32         `json:"vslot_end"`
	Role       string         `json:"role"`
	NodeID     string         `json:"node_id,omitempty"`
	Addr       string         `json:"addr"`
	Weight     uint32         `json:"weight,omitempty"`
	Replicas   []RouteReplica `json:"replicas,omitempty"`
}

type RouteReplica struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
	Weight uint32 `json:"weight,omitempty"`
}

type RoutesBody struct {
	RouteEpoch uint64  `json:"route_epoch"`
	Source     string  `json:"source"`
	Routes     []Route `json:"routes"`
}

type RouteCache struct {
	mu     sync.RWMutex
	epoch  uint64
	source string
	routes []Route
}

func NewRouteCache(source string, routes []Route) *RouteCache {
	return &RouteCache{
		source: source,
		routes: normalizeRoutes(routes),
	}
}

func (c *RouteCache) UpdateFromSnapshot(snapshot controlapi.SnapshotBody, source string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if snapshot.Revision == 0 || c.epoch == snapshot.Revision {
		return false
	}

	routes := make([]Route, 0, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		routes = append(routes, Route{
			Namespace:  route.Namespace,
			Space:      route.Space,
			VSlotStart: route.VSlotStart,
			VSlotEnd:   route.VSlotEnd,
			Role:       "data-node",
			NodeID:     route.NodeID,
			Addr:       route.Addr,
			Weight:     route.Weight,
			Replicas:   routeReplicasFromControl(route.Replicas),
		})
	}

	c.epoch = snapshot.Revision
	c.source = source
	c.routes = normalizeRoutes(routes)
	return true
}

func (c *RouteCache) Snapshot() RoutesBody {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return RoutesBody{
		RouteEpoch: c.epoch,
		Source:     c.source,
		Routes:     cloneRoutes(c.routes),
	}
}

func (c *RouteCache) Select(namespace, space string) (Route, bool) {
	return c.SelectKey(namespace, space, "")
}

func (c *RouteCache) SelectKey(namespace, space, key string) (Route, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vslot := VSlotFor(namespace, space, key)
	var namespaceMatch *Route
	var wildcard *Route
	for i := range c.routes {
		route := &c.routes[i]
		switch selectMatch(route, namespace, space, vslot) {
		case exactRoute:
			return *route, true
		case namespaceRoute:
			if namespaceMatch == nil {
				namespaceMatch = route
			}
		case wildcardRoute:
			if wildcard == nil {
				wildcard = route
			}
		case noRoute:
		}
	}
	return firstRoute(namespaceMatch, wildcard)
}

func VSlotFor(namespace, space, key string) uint32 {
	return routing.VSlotFor(namespace, space, key)
}

type routeMatch uint8

const (
	noRoute routeMatch = iota
	exactRoute
	namespaceRoute
	wildcardRoute
)

func selectMatch(route *Route, namespace, space string, vslot uint32) routeMatch {
	if route.Role != "data-node" || route.Addr == "" {
		return noRoute
	}
	if !route.ContainsVSlot(vslot) {
		return noRoute
	}
	if route.Namespace == namespace && route.Space == space {
		return exactRoute
	}
	if route.Namespace == namespace && route.Space == "" {
		return namespaceRoute
	}
	if route.Namespace == "" && route.Space == "" {
		return wildcardRoute
	}
	return noRoute
}

func (r Route) ContainsVSlot(vslot uint32) bool {
	return routing.ContainsVSlot(controlapi.RouteBody{
		VSlotStart: r.VSlotStart,
		VSlotEnd:   r.VSlotEnd,
	}, vslot)
}

func firstRoute(routes ...*Route) (Route, bool) {
	for _, route := range routes {
		if route == nil {
			continue
		}
		return *route, true
	}
	return Route{}, false
}

func cloneRoutes(routes []Route) []Route {
	out := make([]Route, len(routes))
	copy(out, routes)
	for index := range out {
		out[index].Replicas = cloneRouteReplicas(out[index].Replicas)
	}
	return out
}

func routeReplicasFromControl(replicas []controlapi.RouteReplicaBody) []RouteReplica {
	if len(replicas) == 0 {
		return nil
	}
	out := make([]RouteReplica, 0, len(replicas))
	for index := range replicas {
		out = append(out, RouteReplica{
			NodeID: replicas[index].NodeID,
			Addr:   replicas[index].Addr,
			Weight: replicas[index].Weight,
		})
	}
	return out
}

func cloneRouteReplicas(replicas []RouteReplica) []RouteReplica {
	if len(replicas) == 0 {
		return nil
	}
	out := make([]RouteReplica, len(replicas))
	copy(out, replicas)
	return out
}

func normalizeRoutes(routes []Route) []Route {
	out := cloneRoutes(routes)
	for i := range out {
		if out[i].VSlotStart == 0 && out[i].VSlotEnd == 0 {
			out[i].VSlotEnd = controlapi.VSlotMax
		}
	}
	return out
}
