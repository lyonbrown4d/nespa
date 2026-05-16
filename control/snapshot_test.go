package control_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/control"
)

func TestControlStateSnapshotRestore(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	restored := control.NewControlState("empty")
	if err := restored.RestoreSnapshot(state.ExportSnapshot()); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	snapshot := restored.Snapshot()
	if snapshot.ClusterID != "test" || snapshot.Revision != 4 {
		t.Fatalf("restored snapshot identity = %+v", snapshot)
	}
	if len(snapshot.Routes) != 2 {
		t.Fatalf("restored routes = %+v, want 2", snapshot.Routes)
	}
	if len(restored.MigrationPlans().Plans) != 1 {
		t.Fatalf("restored migration plans = %+v, want 1", restored.MigrationPlans())
	}
}

func TestControlSnapshotFileRoundTrip(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })
	registerNode(t, state)
	createOrdersSession(t, state)

	path := filepath.Join(t.TempDir(), "control", "snapshot.json")
	if err := control.SaveSnapshotFile(path, state.ExportSnapshot()); err != nil {
		t.Fatalf("save snapshot file: %v", err)
	}
	snapshot, err := control.LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("load snapshot file: %v", err)
	}

	restored := control.NewControlState("empty")
	if err := restored.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("restore file snapshot: %v", err)
	}
	if restored.Revision() != state.Revision() {
		t.Fatalf("revision = %d, want %d", restored.Revision(), state.Revision())
	}
}
