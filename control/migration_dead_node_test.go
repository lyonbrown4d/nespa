package control_test

import (
	"slices"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
)

func completeInitialJoinMigration(t *testing.T, state *control.ControlState, now time.Time) {
	t.Helper()

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
}

func heartbeatNode1(t *testing.T, state *control.ControlState) {
	t.Helper()

	if _, err := state.Heartbeat(t.Context(), "node-1", "127.0.0.1:7403"); err != nil {
		t.Fatalf("heartbeat node-1: %v", err)
	}
}

func requireLatestDeadNodeMigrationTask(
	t *testing.T,
	state *control.ControlState,
) controlapi.MigrationTaskBody {
	t.Helper()

	plans := state.MigrationPlans().Plans
	if len(plans) < 2 {
		t.Fatalf("migration plans = %+v, want dead node replanning plan", plans)
	}
	for _, plan := range slices.Backward(plans) {
		if plan.Reason == "node_dead" {
			return requireSingleDeadNodeMigrationTask(t, plan)
		}
	}
	t.Fatalf("migration plans = %+v, want node_dead plan", plans)
	return controlapi.MigrationTaskBody{}
}

func requireSingleDeadNodeMigrationTask(
	t *testing.T,
	plan controlapi.MigrationPlanBody,
) controlapi.MigrationTaskBody {
	t.Helper()

	if len(plan.Tasks) != 1 {
		t.Fatalf("dead migration plan = %+v, want one task", plan)
	}
	task := plan.Tasks[0]
	if task.SourceNodeID != "node-2" || task.TargetNodeID != "node-1" {
		t.Fatalf("dead task nodes = %+v, want node-2 -> node-1", task)
	}
	return task
}

func failAndRetryDeadMigrationTask(
	t *testing.T,
	state *control.ControlState,
	deadTime time.Time,
) controlapi.MigrationTaskBody {
	t.Helper()

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
	return retried
}

func completeDeadMigrationTask(
	t *testing.T,
	state *control.ControlState,
	task controlapi.MigrationTaskBody,
	now time.Time,
) {
	t.Helper()

	if _, err := state.CutoverMigrationTask(task.PlanID, task.TaskID, 100, now); err != nil {
		t.Fatalf("cutover dead task: %v", err)
	}
	if _, err := state.CompleteMigrationTask(task.PlanID, task.TaskID, 100, 100, now); err != nil {
		t.Fatalf("complete dead task: %v", err)
	}
}

func requireRouteNode(t *testing.T, routes []controlapi.RouteBody, key cachewire.Key, wantNodeID, phase string) {
	t.Helper()

	route, ok := routing.Select(routes, key.Namespace, key.Space, key.Key)
	if !ok {
		t.Fatalf("route %s = %+v, want hit", phase, route)
	}
	if route.NodeID != wantNodeID {
		t.Fatalf("route %s = %+v, want %s", phase, route, wantNodeID)
	}
}
