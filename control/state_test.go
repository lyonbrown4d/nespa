package control_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arcgolabs/eventx"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func TestControlStateRegisterNodeBuildsSnapshotRoute(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })

	assertRevision(t, registerNode(t, state), 1)
	assertRevision(t, registerNode(t, state), 1)
	assertSnapshotRoute(t, state.Snapshot(), "test", 1, "node-1", "127.0.0.1:7403")
}

func TestControlStateHeartbeatRegistersUnknownNode(t *testing.T) {
	state := control.NewControlState("test")
	heartbeat, err := state.Heartbeat(t.Context(), "node-2", "127.0.0.1:7503")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	if heartbeat.Revision != 1 {
		t.Fatalf("revision = %d, want 1", heartbeat.Revision)
	}
	if heartbeat.Node.State != "healthy" {
		t.Fatalf("state = %q, want healthy", heartbeat.Node.State)
	}
}

func TestControlStateSnapshotPartitionsVSlotsAcrossHealthyNodes(t *testing.T) {
	state := control.NewControlState("test")
	registerNode(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	routes := state.Snapshot().Routes
	if len(routes) != 2 {
		t.Fatalf("routes len = %d, want 2: %+v", len(routes), routes)
	}
	assertRouteRange(t, routes[0], "node-1", 0, 32767)
	assertRouteRange(t, routes[1], "node-2", 32768, controlapi.VSlotMax)
}

func TestControlStateHeartbeatDoesNotAdvanceRevisionForLivePing(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })
	registerNode(t, state)

	now = time.Unix(20, 0)
	heartbeat, err := state.Heartbeat(t.Context(), "node-1", "127.0.0.1:7403")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

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
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(10, 0) })
	registerNode(t, state)

	suspect := state.AdvanceLiveness(t.Context(), time.Unix(16, 0), 5*time.Second, 10*time.Second)
	if suspect.Revision != 2 {
		t.Fatalf("suspect revision = %d, want 2", suspect.Revision)
	}
	if len(suspect.Changed) != 1 || suspect.Changed[0].State != "suspect" {
		t.Fatalf("unexpected suspect changes: %+v", suspect.Changed)
	}
	if routes := state.Snapshot().Routes; len(routes) != 0 {
		t.Fatalf("suspect node routes len = %d, want 0", len(routes))
	}

	dead := state.AdvanceLiveness(t.Context(), time.Unix(21, 0), 5*time.Second, 10*time.Second)
	if dead.Revision != 3 {
		t.Fatalf("dead revision = %d, want 3", dead.Revision)
	}
	if len(dead.Changed) != 1 || dead.Changed[0].State != "dead" {
		t.Fatalf("unexpected dead changes: %+v", dead.Changed)
	}

	again := state.AdvanceLiveness(t.Context(), time.Unix(30, 0), 5*time.Second, 10*time.Second)
	if again.Revision != 3 {
		t.Fatalf("unchanged revision = %d, want 3", again.Revision)
	}
	if len(again.Changed) != 0 {
		t.Fatalf("unexpected unchanged nodes: %+v", again.Changed)
	}
}

func TestControlStateRecordsRebalanceEvents(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })

	registerNode(t, state)
	events := state.RebalanceEvents()
	if events.Revision != 1 || len(events.Events) != 1 {
		t.Fatalf("registration events = %+v", events)
	}
	if events.Events[0].Reason != "node_registered" || events.Events[0].RouteCount != 1 {
		t.Fatalf("registration event = %+v", events.Events[0])
	}

	now = time.Unix(16, 0)
	state.AdvanceLiveness(t.Context(), now, 5*time.Second, 10*time.Second)
	events = state.RebalanceEvents()
	if events.Revision != 2 || len(events.Events) != 2 {
		t.Fatalf("suspect events = %+v", events)
	}
	last := events.Events[len(events.Events)-1]
	if last.Reason != "node_suspect" || last.State != "suspect" || last.RouteCount != 0 {
		t.Fatalf("suspect event = %+v", last)
	}
}

func TestControlStatePublishesRebalanceEvents(t *testing.T) {
	bus := eventx.New()
	defer func() {
		if err := bus.Close(); err != nil {
			t.Errorf("close bus: %v", err)
		}
	}()

	received := make(chan control.RebalanceEvent, 1)
	if _, err := eventx.Subscribe[control.RebalanceEvent](bus, func(_ context.Context, event control.RebalanceEvent) error {
		received <- event
		return nil
	}); err != nil {
		t.Fatalf("subscribe rebalance event: %v", err)
	}

	now := time.Unix(10, 0)
	state := control.NewControlStateWithClockAndEvents("test", func() time.Time { return now }, bus)
	registerNode(t, state)

	select {
	case event := <-received:
		if event.Event.Reason != "node_registered" {
			t.Fatalf("event reason = %q, want node_registered", event.Event.Reason)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rebalance event")
	}
}

func TestControlStateHeartbeatRecoversSuspectNode(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })
	registerNode(t, state)
	state.AdvanceLiveness(t.Context(), time.Unix(16, 0), 5*time.Second, 10*time.Second)

	now = time.Unix(17, 0)
	heartbeat, err := state.Heartbeat(t.Context(), "node-1", "127.0.0.1:7403")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	if heartbeat.Revision != 3 {
		t.Fatalf("revision = %d, want 3", heartbeat.Revision)
	}
	if heartbeat.Node.State != "healthy" {
		t.Fatalf("node state = %q, want healthy", heartbeat.Node.State)
	}
	if routes := state.Snapshot().Routes; len(routes) != 1 {
		t.Fatalf("recovered node routes len = %d, want 1", len(routes))
	}
}

func TestControlStateRejectsInvalidNodeIdentity(t *testing.T) {
	state := control.NewControlState("test")
	for _, test := range []struct {
		name   string
		nodeID string
		addr   string
	}{
		{name: "missing node id", addr: "127.0.0.1:7403"},
		{name: "missing addr", nodeID: "node-1"},
		{name: "missing port", nodeID: "node-1", addr: "127.0.0.1"},
		{name: "scheme", nodeID: "node-1", addr: "tcp://127.0.0.1:7403"},
		{name: "zero port", nodeID: "node-1", addr: "127.0.0.1:0"},
		{name: "nul node id", nodeID: "node\x00-1", addr: "127.0.0.1:7403"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := state.RegisterNode(t.Context(), test.nodeID, test.addr)
			if !errors.Is(err, control.ErrInvalidNode) {
				t.Fatalf("err = %v, want ErrInvalidNode", err)
			}
		})
	}
	if nodes := state.Nodes(); len(nodes.Nodes) != 0 {
		t.Fatalf("invalid nodes registered: %+v", nodes.Nodes)
	}
}

func registerNode(t *testing.T, state *control.ControlState) uint64 {
	t.Helper()
	return registerSpecificNode(t, state, "node-1", "127.0.0.1:7403")
}

func registerSpecificNode(t *testing.T, state *control.ControlState, nodeID, addr string) uint64 {
	t.Helper()
	response, err := state.RegisterNode(t.Context(), nodeID, addr)
	if err != nil {
		t.Fatalf("register node: %v", err)
	}
	return response.Revision
}

func assertRevision(t *testing.T, got, want uint64) {
	t.Helper()
	if got != want {
		t.Fatalf("revision = %d, want %d", got, want)
	}
}

func assertSnapshotRoute(t *testing.T, snapshot controlapi.SnapshotBody, clusterID string, revision uint64, nodeID, addr string) {
	t.Helper()
	if snapshot.ClusterID != clusterID || snapshot.Revision != revision {
		t.Fatalf("unexpected snapshot identity: %+v", snapshot)
	}
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(snapshot.Nodes))
	}
	if len(snapshot.Routes) != 1 {
		t.Fatalf("routes len = %d, want 1", len(snapshot.Routes))
	}
	if snapshot.Routes[0].NodeID != nodeID || snapshot.Routes[0].Addr != addr {
		t.Fatalf("unexpected route: %+v", snapshot.Routes[0])
	}
	assertRouteRange(t, snapshot.Routes[0], nodeID, 0, controlapi.VSlotMax)
}

func assertRouteRange(t *testing.T, route controlapi.RouteBody, nodeID string, start, end uint32) {
	t.Helper()
	if route.NodeID != nodeID || route.VSlotStart != start || route.VSlotEnd != end {
		t.Fatalf("route = %+v, want node=%s range=%d-%d", route, nodeID, start, end)
	}
}
