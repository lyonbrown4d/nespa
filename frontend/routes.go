package frontend

import (
	"sync"

	"github.com/lyonbrown4d/nespa/controlapi"
)

type Route struct {
	Namespace string `json:"namespace,omitempty"`
	Space     string `json:"space,omitempty"`
	Role      string `json:"role"`
	NodeID    string `json:"node_id,omitempty"`
	Addr      string `json:"addr"`
	Weight    uint32 `json:"weight,omitempty"`
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
		routes: cloneRoutes(routes),
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
			Namespace: route.Namespace,
			Space:     route.Space,
			Role:      "data-node",
			NodeID:    route.NodeID,
			Addr:      route.Addr,
			Weight:    route.Weight,
		})
	}

	c.epoch = snapshot.Revision
	c.source = source
	c.routes = routes
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	var namespaceMatch *Route
	var wildcard *Route
	for i := range c.routes {
		route := &c.routes[i]
		switch selectMatch(route, namespace, space) {
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

type routeMatch uint8

const (
	noRoute routeMatch = iota
	exactRoute
	namespaceRoute
	wildcardRoute
)

func selectMatch(route *Route, namespace, space string) routeMatch {
	if route.Role != "data-node" || route.Addr == "" {
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
	return out
}
