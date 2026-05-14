package node_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/node"
)

func TestServiceRuntimeObservesRegistrationRevision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/control/nodes" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("content-type", "application/json")
		if err := json.NewEncoder(w).Encode(controlapi.RegisterNodeResponse{Revision: 3}); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer server.Close()

	svc := node.NewServiceRuntime(node.Config{
		Addr:              "127.0.0.1:7403",
		ControlAddr:       server.URL,
		NodeID:            "node-1",
		HeartbeatInterval: time.Hour,
	}, nil)
	if err := node.StartControlRegistration(t.Context(), slog.New(slog.DiscardHandler), svc); err != nil {
		t.Fatalf("start control registration: %v", err)
	}

	if got := svc.RouteEpoch(); got != 3 {
		t.Fatalf("route epoch = %d, want 3", got)
	}
}
