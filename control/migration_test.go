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

	joinTask := state.ClaimMigrationTask(now)
	if !joinTask.Claimed {
		t.Fatalf("expected join task claim before dead-path test")
	}
	if _, err := state.CutoverMigrationTask(joinTask.Task.PlanID, joinTask.Task.TaskID, 0, now); err != nil {
		t.Fatalf("cutover join task: %v", err)
	}
	if _, err := state.CompleteMigrationTask(joinTask.Task.PlanID, joinTask.Task.TaskID, 0, 0, now); err != nil {
		t.Fatalf("complete join task: %v", err)
	}

	now = time.Unix(40, 0)
	if _, err := state.Heartbeat(t.Context(), "node-1", "127.0.0.1:7403"); err != nil {
		t.Fatalf("heartbeat node-1: %v", err)
	}

	deadTime := time.Unix(45, 0)
	now = deadTime
	state.AdvanceLiveness(t.Context(), deadTime, 0, 10*time.Second)

	plans := state.MigrationPlans().Plans
	if len(plans) < 2 {
		t.Fatalf("migration plans = %+v, want dead node replanning plan", plans)
	}
	var deadPlan controlapi.MigrationPlanBody
	found := false
	for index := len(plans) - 1; index >= 0; index-- {
		if plans[index].Reason == "node_dead" {
			deadPlan = plans[index]
			found = true
			break
		}
	}
	if !found || len(deadPlan.Tasks) != 1 {
		t.Fatalf("dead migration plan = %+v, want one task", deadPlan)
	}
	deadTask := deadPlan.Tasks[0]
	if deadTask.SourceNodeID != "node-2" || deadTask.TargetNodeID != "node-1" {
		t.Fatalf("dead task nodes = %+v, want node-2 -> node-1", deadTask)
	}

	snapshot := state.Snapshot()
	key := keyInTaskRange(t, deadTask)
	before, ok := routing.Select(snapshot.Routes, key.Namespace, key.Space, key.Key)
	if !ok {
		t.Fatalf("route before cutover = %+v, want hit", before)
	}
	if before.NodeID != "node-2" {
		t.Fatalf("route before cutover = %+v, want node-2", before)
	}

	claimed := state.ClaimMigrationTask(deadTime)
	if !claimed.Claimed {
		t.Fatalf("expected claim dead task")
	}
	claimedTask := claimed.Task

	failed, err := state.FailMigrationTask(
		claimedTask.PlanID,
		claimedTask.TaskID,
		"temporary network",
		2*time.Second,
		deadTime,
	)
	if err != nil {
		t.Fatalf("fail dead task: %v", err)
	}
	if failed.Attempts != 1 || failed.State != "failed" {
		t.Fatalf("failed task = %+v, want attempts=1 failed", failed)
	}

	retryable := state.ClaimMigrationTask(deadTime.Add(time.Second))
	if retryable.Claimed {
		t.Fatalf("expected no claim before retry, got %+v", retryable)
	}
	retryable = state.ClaimMigrationTask(deadTime.Add(3 * time.Second))
	if !retryable.Claimed {
		t.Fatalf("expected claim after retry, got %+v", retryable)
	}

	retried := retryable.Task
	if retried.State != "running" {
		t.Fatalf("retried task state = %s, want running", retried.State)
	}
	if _, err := state.CutoverMigrationTask(retried.PlanID, retried.TaskID, 100, deadTime.Add(3*time.Second)); err != nil {
		t.Fatalf("cutover dead task: %v", err)
	}
	if _, err := state.CompleteMigrationTask(
		retried.PlanID,
		retried.TaskID,
		100,
		100,
		deadTime.Add(3*time.Second),
	); err != nil {
		t.Fatalf("complete dead task: %v", err)
	}

	after, ok := routing.Select(state.Snapshot().Routes, key.Namespace, key.Space, key.Key)
	if !ok {
		t.Fatalf("route after complete = %+v, want hit", after)
	}
	if after.NodeID != "node-1" {
		t.Fatalf("route after complete = %+v, want node-1", after)
	}
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
