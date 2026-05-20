package node_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/node"
)

func TestServiceRuntimeObservesRegistrationRevision(t *testing.T) {
	server := newServiceRuntimeControlServer(t, controlapi.SnapshotBody{Revision: 3})
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

func TestServiceRuntimeReplicationTargetsOnlyForPrimary(t *testing.T) {
	key := cachewireKey("orders", "session", "k")
	snapshot := controlapi.SnapshotBody{
		Revision: 7,
		Routes: []controlapi.RouteBody{
			{
				Namespace:  key.Namespace,
				Space:      key.Space,
				VSlotStart: 0,
				VSlotEnd:   controlapi.VSlotMax,
				NodeID:     "node-1",
				Addr:       "127.0.0.1:7403",
				Replicas: []controlapi.RouteReplicaBody{
					{NodeID: "node-2", Addr: "127.0.0.1:7503"},
					{NodeID: "node-1", Addr: "127.0.0.1:7403"},
				},
			},
		},
	}
	server := newServiceRuntimeControlServer(t, snapshot)
	defer server.Close()

	primary := startServiceRuntime(t, server.URL, "node-1", "127.0.0.1:7403")
	targets := primary.ReplicationTargets(key)
	if len(targets) != 1 || targets[0] != "127.0.0.1:7503" {
		t.Fatalf("replication targets = %v, want [127.0.0.1:7503]", targets)
	}

	replica := startServiceRuntime(t, server.URL, "node-2", "127.0.0.1:7503")
	if got := replica.ReplicationTargets(key); len(got) != 0 {
		t.Fatalf("replica should not return replication targets: %v", got)
	}
}

func startServiceRuntime(t *testing.T, controlAddr, nodeID, addr string) *node.ServiceRuntime {
	t.Helper()
	svc := node.NewServiceRuntime(node.Config{
		Addr:              addr,
		ControlAddr:       controlAddr,
		NodeID:            nodeID,
		HeartbeatInterval: time.Hour,
	}, nil)
	if err := node.StartControlRegistration(t.Context(), slog.New(slog.DiscardHandler), svc); err != nil {
		t.Fatalf("start control registration: %v", err)
	}
	return svc
}

func newServiceRuntimeControlServer(t *testing.T, snapshot controlapi.SnapshotBody) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/control/nodes":
			var body controlapi.RegisterNodeBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode register body: %v", err)
			}
			writeServiceRuntimeJSON(t, w, controlapi.RegisterNodeResponse{
				Revision: snapshot.Revision,
				Node:     controlapi.NodeBody{NodeID: body.NodeID, Addr: body.Addr, State: "healthy"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/control/snapshot":
			writeServiceRuntimeJSON(t, w, snapshot)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
}

func cachewireKey(namespace, space, key string) cachewire.Key {
	return cachewire.Key{Namespace: namespace, Space: space, Key: key}
}

func writeServiceRuntimeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write response: %v", err)
	}
}
