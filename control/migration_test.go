package control_test

import (
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func TestControlStatePlansRouteMigrationOnNodeJoin(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	plans := state.MigrationPlans()
	if plans.Revision != 4 || len(plans.Plans) != 1 {
		t.Fatalf("migration plans = %+v, want one plan at revision 4", plans)
	}
	plan := plans.Plans[0]
	if plan.State != "planned" || plan.Reason != "node_registered" || len(plan.Tasks) != 1 {
		t.Fatalf("migration plan = %+v, want one planned node_registered task", plan)
	}
	assertMigrationTask(t, plan.Tasks[0])
}

func assertMigrationTask(t *testing.T, task controlapi.MigrationTaskBody) {
	t.Helper()
	if task.SourceNodeID != "node-1" || task.TargetNodeID != "node-2" {
		t.Fatalf("migration task nodes = %+v, want node-1 -> node-2", task)
	}
	if task.Namespace != "orders" || task.Space != "session" || task.VSlotStart != 32768 || task.VSlotEnd != controlapi.VSlotMax {
		t.Fatalf("migration task range = %+v, want orders/session 32768-%d", task, controlapi.VSlotMax)
	}
}
