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

func TestControlStateHeartbeatDoesNotAdvanceRevisionForLivePing(t *testing.T) {
	state := NewControlState("test")
	state.now = func() time.Time { return time.Unix(10, 0) }
	state.RegisterNode("node-1", "127.0.0.1:7403")

	state.now = func() time.Time { return time.Unix(20, 0) }
	heartbeat := state.Heartbeat("node-1", "127.0.0.1:7403")

	if heartbeat.Revision != 1 {
		t.Fatalf("revision = %d, want 1", heartbeat.Revision)
	}
	nodes := state.Nodes()
	if len(nodes.Nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes.Nodes))
	}
	if nodes.Nodes[0].LastSeenUnix != 20 {
		t.Fatalf("last seen = %d, want 20", nodes.Nodes[0].LastSeenUnix)
	}
}

func TestControlStateAdvanceLivenessTransitionsNodeState(t *testing.T) {
	state := NewControlState("test")
	state.now = func() time.Time { return time.Unix(10, 0) }
	state.RegisterNode("node-1", "127.0.0.1:7403")

	suspect := state.AdvanceLiveness(time.Unix(16, 0), 5*time.Second, 10*time.Second)
	if suspect.Revision != 2 {
		t.Fatalf("suspect revision = %d, want 2", suspect.Revision)
	}
	if len(suspect.Changed) != 1 || suspect.Changed[0].State != nodeStateSuspect {
		t.Fatalf("unexpected suspect changes: %+v", suspect.Changed)
	}
	if routes := state.Snapshot().Routes; len(routes) != 0 {
		t.Fatalf("suspect node routes len = %d, want 0", len(routes))
	}

	dead := state.AdvanceLiveness(time.Unix(21, 0), 5*time.Second, 10*time.Second)
	if dead.Revision != 3 {
		t.Fatalf("dead revision = %d, want 3", dead.Revision)
	}
	if len(dead.Changed) != 1 || dead.Changed[0].State != nodeStateDead {
		t.Fatalf("unexpected dead changes: %+v", dead.Changed)
	}

	again := state.AdvanceLiveness(time.Unix(30, 0), 5*time.Second, 10*time.Second)
	if again.Revision != 3 {
		t.Fatalf("unchanged revision = %d, want 3", again.Revision)
	}
	if len(again.Changed) != 0 {
		t.Fatalf("unexpected unchanged nodes: %+v", again.Changed)
	}
}

func TestControlStateHeartbeatRecoversSuspectNode(t *testing.T) {
	state := NewControlState("test")
	state.now = func() time.Time { return time.Unix(10, 0) }
	state.RegisterNode("node-1", "127.0.0.1:7403")
	state.AdvanceLiveness(time.Unix(16, 0), 5*time.Second, 10*time.Second)

	state.now = func() time.Time { return time.Unix(17, 0) }
	heartbeat := state.Heartbeat("node-1", "127.0.0.1:7403")

	if heartbeat.Revision != 3 {
		t.Fatalf("revision = %d, want 3", heartbeat.Revision)
	}
	if heartbeat.Node.State != nodeStateHealthy {
		t.Fatalf("node state = %q, want %q", heartbeat.Node.State, nodeStateHealthy)
	}
	if routes := state.Snapshot().Routes; len(routes) != 1 {
		t.Fatalf("recovered node routes len = %d, want 1", len(routes))
	}
}
