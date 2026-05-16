package control_test

import (
	"net/http"
	"testing"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/control"
)

func TestControlEndpointsRegisterRoutes(t *testing.T) {
	svc := control.NewServiceRuntime(control.Config{ClusterID: "test"})
	server := httpx.New()

	server.RegisterOnly(
		control.NewReadEndpoint(svc),
		control.NewCatalogEndpoint(svc),
		control.NewNodeEndpoint(svc),
	)

	for _, route := range controlEndpointRoutes() {
		if !server.HasRoute(route.method, route.path) {
			t.Fatalf("route %s %s was not registered", route.method, route.path)
		}
	}
}

func controlEndpointRoutes() []struct {
	method string
	path   string
} {
	return []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/v1/control/state"},
		{method: http.MethodGet, path: "/v1/control/snapshot"},
		{method: http.MethodGet, path: "/v1/control/rebalance/events"},
		{method: http.MethodGet, path: "/v1/control/rebalance/plans"},
		{method: http.MethodGet, path: "/v1/control/namespaces"},
		{method: http.MethodPost, path: "/v1/control/namespaces"},
		{method: http.MethodPost, path: "/v1/control/namespaces/version-bump"},
		{method: http.MethodGet, path: "/v1/control/spaces"},
		{method: http.MethodPost, path: "/v1/control/spaces"},
		{method: http.MethodPost, path: "/v1/control/spaces/version-bump"},
		{method: http.MethodGet, path: "/v1/control/entities"},
		{method: http.MethodPost, path: "/v1/control/entities"},
		{method: http.MethodGet, path: "/v1/control/nodes"},
		{method: http.MethodPost, path: "/v1/control/nodes"},
		{method: http.MethodPut, path: "/v1/control/nodes/heartbeat"},
	}
}
