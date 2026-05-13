package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lyonbrown4d/nespa/internal/controlapi"
)

func TestControlClientRegisterAndHeartbeat(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		w.Header().Set("content-type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/control/nodes":
			var body controlapi.RegisterNodeBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode register body: %v", err)
			}
			if body.NodeID != "node-1" || body.Addr != "127.0.0.1:7403" {
				t.Fatalf("unexpected register body: %+v", body)
			}
			writeControlJSON(t, w, controlapi.RegisterNodeResponse{
				Revision: 1,
				Node: controlapi.NodeBody{
					NodeID: body.NodeID,
					Addr:   body.Addr,
					State:  "healthy",
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/control/nodes/heartbeat":
			var body controlapi.HeartbeatBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode heartbeat body: %v", err)
			}
			if body.NodeID != "node-1" || body.Addr != "127.0.0.1:7403" {
				t.Fatalf("unexpected heartbeat body: %+v", body)
			}
			writeControlJSON(t, w, controlapi.HeartbeatResponse{
				Revision: 1,
				Node: controlapi.NodeBody{
					NodeID: body.NodeID,
					Addr:   body.Addr,
					State:  "healthy",
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewControlClient(server.URL)
	ctx := context.Background()

	register, err := client.RegisterNode(ctx, controlapi.RegisterNodeBody{NodeID: "node-1", Addr: "127.0.0.1:7403"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if register.Revision != 1 || register.Node.State != "healthy" {
		t.Fatalf("unexpected register response: %+v", register)
	}

	heartbeat, err := client.Heartbeat(ctx, controlapi.HeartbeatBody{NodeID: "node-1", Addr: "127.0.0.1:7403"})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if heartbeat.Revision != 1 || heartbeat.Node.State != "healthy" {
		t.Fatalf("unexpected heartbeat response: %+v", heartbeat)
	}

	want := []string{"POST /v1/control/nodes", "PUT /v1/control/nodes/heartbeat"}
	for i, item := range want {
		if seen[i] != item {
			t.Fatalf("seen[%d] = %q, want %q", i, seen[i], item)
		}
	}
}

func writeControlJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write json: %v", err)
	}
}
