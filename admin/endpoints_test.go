package admin_test

import (
	"net/http"
	"testing"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/control"
)

func TestAdminEndpointRegistersRoutes(t *testing.T) {
	server := httpx.New()

	var (
		cacheSvc   cache.Service
		controlSvc *control.ServiceRuntime
	)
	server.RegisterOnly(admin.NewSummaryEndpoint(admin.Config{}, cacheSvc, controlSvc))

	if !server.HasRoute(http.MethodGet, "/v1/admin/summary") {
		t.Fatal("route GET /v1/admin/summary was not registered")
	}
}
