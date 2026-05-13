package node_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/node"
)

func TestControlClientRegisterAndHeartbeat(t *testing.T) {
	var seen []string
	server := newControlClientTestServer(t, &seen)
	defer server.Close()

	client := node.NewControlClient(server.URL)
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

	assertSeenRequests(t, seen, []string{"POST /v1/control/nodes", "PUT /v1/control/nodes/heartbeat"})
}

func newControlClientTestServer(t *testing.T, seen *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*seen = append(*seen, r.Method+" "+r.URL.Path)
		w.Header().Set("content-type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/control/nodes":
			handleRegister(t, w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/control/nodes/heartbeat":
			handleHeartbeat(t, w, r)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
}

func handleRegister(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var body controlapi.RegisterNodeBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode register body: %v", err)
	}
	assertNodeIdentity(t, body.NodeID, body.Addr)
	writeControlJSON(t, w, controlapi.RegisterNodeResponse{
		Revision: 1,
		Node:     healthyNode(body.NodeID, body.Addr),
	})
}

func handleHeartbeat(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var body controlapi.HeartbeatBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode heartbeat body: %v", err)
	}
	assertNodeIdentity(t, body.NodeID, body.Addr)
	writeControlJSON(t, w, controlapi.HeartbeatResponse{
		Revision: 1,
		Node:     healthyNode(body.NodeID, body.Addr),
	})
}

func assertNodeIdentity(t *testing.T, nodeID, addr string) {
	t.Helper()
	if nodeID != "node-1" || addr != "127.0.0.1:7403" {
		t.Fatalf("unexpected node identity: %s %s", nodeID, addr)
	}
}

func healthyNode(nodeID, addr string) controlapi.NodeBody {
	return controlapi.NodeBody{
		NodeID: nodeID,
		Addr:   addr,
		State:  "healthy",
	}
}

func assertSeenRequests(t *testing.T, seen, want []string) {
	t.Helper()
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
