// Package routing contains shared route-table helpers for SDKs and frontend views.
package routing

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func VSlotFor(namespace, space, key string) uint32 {
	hash := fnv.New64a()
	writeHashPart(hash, namespace)
	writeHashPart(hash, space)
	writeHashPart(hash, key)
	return uint32(hash.Sum64() % uint64(controlapi.VSlotCount))
}

func Select(routes []controlapi.RouteBody, namespace, space, key string) (controlapi.RouteBody, bool) {
	vslot := VSlotFor(namespace, space, key)
	var candidates routeCandidates

	for index := range routes {
		route := &routes[index]
		if !selectable(route, vslot) {
			continue
		}
		if routeMatch(route, namespace, space) == exactRoute {
			return *route, true
		}
		candidates.add(routeMatch(route, namespace, space), route)
	}
	return firstRoute(candidates.namespaceMatch, candidates.wildcard)
}

func NamespaceVersion(namespaces []controlapi.NamespaceBody, namespace string) (uint64, bool) {
	if len(namespaces) == 0 {
		return 0, true
	}
	for _, item := range namespaces {
		if item.Namespace == namespace {
			return item.Version, true
		}
	}
	return 0, false
}

func SpaceVersion(spaces []controlapi.SpaceBody, namespace, space string) (uint64, bool) {
	if len(spaces) == 0 {
		return 0, true
	}
	for _, item := range spaces {
		if item.Namespace == namespace && item.Space == space {
			return item.Version, true
		}
	}
	return 0, false
}

func ContainsVSlot(route controlapi.RouteBody, vslot uint32) bool {
	start, end := normalizedRange(route)
	return vslot >= start && vslot <= end
}

type matchKind uint8

type routeCandidates struct {
	namespaceMatch *controlapi.RouteBody
	wildcard       *controlapi.RouteBody
}

const (
	noRoute matchKind = iota
	exactRoute
	namespaceRoute
	wildcardRoute
)

func selectable(route *controlapi.RouteBody, vslot uint32) bool {
	return route.Addr != "" && ContainsVSlot(*route, vslot)
}

func (c *routeCandidates) add(kind matchKind, route *controlapi.RouteBody) {
	switch kind {
	case namespaceRoute:
		if c.namespaceMatch == nil {
			c.namespaceMatch = route
		}
	case wildcardRoute:
		if c.wildcard == nil {
			c.wildcard = route
		}
	case exactRoute, noRoute:
	}
}

func routeMatch(route *controlapi.RouteBody, namespace, space string) matchKind {
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

func firstRoute(routes ...*controlapi.RouteBody) (controlapi.RouteBody, bool) {
	for _, route := range routes {
		if route != nil {
			return *route, true
		}
	}
	return controlapi.RouteBody{}, false
}

func normalizedRange(route controlapi.RouteBody) (uint32, uint32) {
	if route.VSlotStart == 0 && route.VSlotEnd == 0 {
		return 0, controlapi.VSlotMax
	}
	return route.VSlotStart, route.VSlotEnd
}

func writeHashPart(hash interface{ Write([]byte) (int, error) }, value string) {
	var size []byte
	size = binary.AppendUvarint(size, uint64(len(value)))
	if _, err := hash.Write(size); err != nil {
		return
	}
	if _, err := hash.Write([]byte(value)); err != nil {
		return
	}
}
