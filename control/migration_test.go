package control_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
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

func TestControlStateMigrationTaskRoutesPreferSourceUntilCutover(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	plans := state.MigrationPlans().Plans
	task := plans[0].Tasks[0]
	key := keyInTaskRange(t, task)

	routeBefore, ok := routing.Select(state.Snapshot().Routes, key.Namespace, key.Space, key.Key)
	if !ok {
		t.Fatalf("routing before cutover = %+v, want hit", routeBefore)
	}
	if routeBefore.NodeID != "node-1" {
		t.Fatalf("route before cutover = %+v, want node-1", routeBefore)
	}

	if _, err := state.CutoverMigrationTask(task.PlanID, task.TaskID, 0, time.Unix(124, 0)); err != nil {
		t.Fatalf("cutover migration task: %v", err)
	}
	routeAfter, ok := routing.Select(state.Snapshot().Routes, key.Namespace, key.Space, key.Key)
	if !ok {
		t.Fatalf("routing after cutover = %+v, want hit", routeAfter)
	}
	if routeAfter.NodeID != "node-2" {
		t.Fatalf("route after cutover = %+v, want node-2", routeAfter)
	}
}

func TestControlStateFailedMigrationTaskHonorsRetryBackoff(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	claimed := state.ClaimMigrationTask(now)
	if !claimed.Claimed {
		t.Fatal("expected first claim")
	}

	task := claimed.Task
	_, err := state.FailMigrationTask(task.PlanID, task.TaskID, "boom", time.Second, now)
	if err != nil {
		t.Fatalf("fail migration task: %v", err)
	}

	retried := state.ClaimMigrationTask(now)
	if retried.Claimed {
		t.Fatalf("expected no claim before retry, got %+v", retried)
	}

	if _, err := state.FailMigrationTask(task.PlanID, task.TaskID, "still bad", 2*time.Second, now.Add(3*time.Second)); err != nil {
		t.Fatalf("fail migration task again: %v", err)
	}

	again := state.ClaimMigrationTask(now.Add(5 * time.Second))
	if !again.Claimed {
		t.Fatalf("expected retryable claim after backoff, got %+v", again)
	}
}

func TestControlStateTimedOutMigrationTasks(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")

	claimed := state.ClaimMigrationTask(now)
	if !claimed.Claimed {
		t.Fatal("expected join task claim")
	}
	taskID := claimed.Task.TaskID
	planID := claimed.Task.PlanID

	stillRunning := state.TimedOutMigrationTasks(now.Add(9*time.Second), 10*time.Second)
	if len(stillRunning) != 0 {
		t.Fatalf("timed-out tasks = %+v, want none", stillRunning)
	}

	markedCutover := now.Add(1 * time.Second)
	if _, err := state.CutoverMigrationTask(planID, taskID, 0, markedCutover); err != nil {
		t.Fatalf("cutover migration task: %v", err)
	}

	if notStale := state.TimedOutMigrationTasks(markedCutover.Add(8*time.Second), 10*time.Second); len(notStale) != 0 {
		t.Fatalf("timed-out tasks = %+v, want none before timeout", notStale)
	}

	stale := state.TimedOutMigrationTasks(markedCutover.Add(9*time.Second), 10*time.Second)
	if len(stale) != 1 {
		t.Fatalf("timed-out tasks = %+v, want one cleanup task", stale)
	}
	if stale[0].State != "cleanup" {
		t.Fatalf("timed-out task = %+v, want cleanup state", stale[0])
	}
}

func TestControlStateDeadNodeMigrationTaskRetryThenCompletes(t *testing.T) {
	now := time.Unix(10, 0)
	state := control.NewControlStateWithClock("test", func() time.Time { return now })
	registerNode(t, state)
	createOrdersSession(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")
	completeInitialJoinMigration(t, state, now)

	deadTime := time.Unix(45, 0)
	now = time.Unix(40, 0)
	heartbeatNode1(t, state)
	now = deadTime
	state.AdvanceLiveness(t.Context(), deadTime, 0, 10*time.Second)

	deadTask := requireLatestDeadNodeMigrationTask(t, state)
	key := keyInTaskRange(t, deadTask)
	requireRouteNode(t, state.Snapshot().Routes, key, "node-2", "before cutover")

	retried := failAndRetryDeadMigrationTask(t, state, deadTime)
	completeDeadMigrationTask(t, state, retried, deadTime.Add(3*time.Second))
	requireRouteNode(t, state.Snapshot().Routes, key, "node-1", "after complete")
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

func keyInTaskRange(t *testing.T, task controlapi.MigrationTaskBody) cachewire.Key {
	t.Helper()
	for index := range 100_000 {
		key := cachewire.Key{
			Namespace: task.Namespace,
			Space:     task.Space,
			Entity:    "SessionView",
			Key:       fmt.Sprintf("session-migration-test-%d", index),
		}
		slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
		if slot >= task.VSlotStart && slot <= task.VSlotEnd {
			return key
		}
	}
	t.Fatal("failed to find key in task range")
	return cachewire.Key{}
}
