package control_test

import (
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/control"
)

func TestControlFSMAppliesCatalogAndNodeCommands(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })
	fsm := control.NewControlFSM(state)

	if _, err := fsm.Apply(t.Context(), control.Command{Type: control.CommandRegisterNode, NodeID: "node-1", Addr: "127.0.0.1:7403"}); err != nil {
		t.Fatalf("apply register node: %v", err)
	}
	if _, err := fsm.Apply(t.Context(), control.Command{Type: control.CommandCreateNamespace, Namespace: "orders"}); err != nil {
		t.Fatalf("apply namespace: %v", err)
	}
	if _, err := fsm.Apply(t.Context(), control.Command{Type: control.CommandCreateSpace, Namespace: "orders", Space: "session"}); err != nil {
		t.Fatalf("apply space: %v", err)
	}

	snapshot := state.Snapshot()
	if snapshot.Revision != 3 || len(snapshot.Routes) != 1 {
		t.Fatalf("snapshot after fsm commands = %+v", snapshot)
	}
}

func TestControlFSMAdvancesLiveness(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(10, 0) })
	registerNode(t, state)
	fsm := control.NewControlFSM(state)

	result, err := fsm.Apply(t.Context(), control.Command{
		Type:           control.CommandAdvanceNodeLiveness,
		NowUnix:        16,
		SuspectAfterMS: (5 * time.Second).Milliseconds(),
		DeadAfterMS:    (10 * time.Second).Milliseconds(),
	})
	if err != nil {
		t.Fatalf("apply liveness: %v", err)
	}
	if result.Liveness.Revision != 2 || len(result.Liveness.Changed) != 1 {
		t.Fatalf("liveness result = %+v", result.Liveness)
	}
}
