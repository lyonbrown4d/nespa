package admin_test

import (
	"net/http"
	"testing"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/admin"
)

func TestAdminEndpointRegistersRoutes(t *testing.T) {
	server := httpx.New()

	server.RegisterOnly(admin.NewSummaryEndpoint(admin.Config{}))

	if !server.HasRoute(http.MethodGet, "/v1/admin/summary") {
		t.Fatal("route GET /v1/admin/summary was not registered")
	}
}
