package control

import (
	"testing"
	"time"
)

func TestControlStateRegisterNodeBuildsSnapshotRoute(t *testing.T) {
	state := NewControlState("test")
	state.now = func() time.Time { return time.Unix(123, 0) }

	first := state.RegisterNode("node-1", "127.0.0.1:7403")
	if first.Revision != 1 {
		t.Fatalf("revision = %d, want 1", first.Revision)
	}

	again := state.RegisterNode("node-1", "127.0.0.1:7403")
	if again.Revision != 1 {
		t.Fatalf("same node revision = %d, want 1", again.Revision)
	}

	snapshot := state.Snapshot()
	if snapshot.ClusterID != "test" || snapshot.Revision != 1 {
		t.Fatalf("unexpected snapshot identity: %+v", snapshot)
	}
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(snapshot.Nodes))
	}
	if len(snapshot.Routes) != 1 {
		t.Fatalf("routes len = %d, want 1", len(snapshot.Routes))
	}
	if snapshot.Routes[0].NodeID != "node-1" || snapshot.Routes[0].Addr != "127.0.0.1:7403" {
		t.Fatalf("unexpected route: %+v", snapshot.Routes[0])
	}
}

func TestControlStateHeartbeatRegistersUnknownNode(t *testing.T) {
	state := NewControlState("test")
	heartbeat := state.Heartbeat("node-2", "127.0.0.1:7503")

	if heartbeat.Revision != 1 {
		t.Fatalf("revision = %d, want 1", heartbeat.Revision)
	}
	if heartbeat.Node.State != nodeStateHealthy {
		t.Fatalf("state = %q, want %q", heartbeat.Node.State, nodeStateHealthy)
	}
}
