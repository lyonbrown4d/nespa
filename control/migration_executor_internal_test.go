package control

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func TestMigrateRangeCleanupUnfencesSource(t *testing.T) {
	t.Parallel()

	client := &fakeMigrationRangeClient{}
	task := controlapi.MigrationTaskBody{
		PlanID:          1,
		TaskID:          1,
		SourceAddr:      "source",
		TargetAddr:      "target",
		CutoverAtUnix:   123,
		ImportedEntries: 11,
	}
	imported, deleted, err := migrateRange(t.Context(), nil, client, MigrationConfig{
		TaskTimeout: time.Second,
	}, task)
	if err != nil {
		t.Fatalf("migrate cleanup range: %v", err)
	}
	if imported != 11 {
		t.Fatalf("imported=%d, want 11", imported)
	}
	if deleted != 3 {
		t.Fatalf("deleted=%d, want 3", deleted)
	}
	if !slices.Equal(client.calls, []string{"delete", "unfence"}) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{"delete", "unfence"})
	}
}

func TestMigrateRangeCleanupPropagatesUnfenceError(t *testing.T) {
	t.Parallel()

	wantUnfenceErr := errors.New("temporary network")
	client := &fakeMigrationRangeClient{
		unfenceErrs: []error{wantUnfenceErr},
	}
	task := controlapi.MigrationTaskBody{
		PlanID:          1,
		TaskID:          1,
		SourceAddr:      "source",
		TargetAddr:      "target",
		CutoverAtUnix:   123,
		ImportedEntries: 7,
	}
	_, _, err := migrateRange(t.Context(), nil, client, MigrationConfig{
		TaskTimeout: time.Second,
	}, task)
	if err == nil {
		t.Fatal("expected unfence error")
	}
	if !errors.Is(err, wantUnfenceErr) {
		t.Fatalf("err = %v, want unfence error", err)
	}
	if !slices.Equal(client.calls, []string{"delete", "unfence"}) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{"delete", "unfence"})
	}
}

func TestMigrateRangeCleanupUnfenceCalledEvenWhenDeleteFails(t *testing.T) {
	t.Parallel()

	deleteErr := errors.New("delete temporary failure")
	client := &fakeMigrationRangeClient{
		deleteErrs: []error{deleteErr},
	}
	task := controlapi.MigrationTaskBody{
		PlanID:        1,
		TaskID:        1,
		SourceAddr:    "source",
		TargetAddr:    "target",
		CutoverAtUnix: 1,
	}
	_, _, err := migrateRange(t.Context(), nil, client, MigrationConfig{
		TaskTimeout: time.Second,
	}, task)
	if err == nil {
		t.Fatal("expected cleanup error")
	}
	if !errors.Is(err, deleteErr) {
		t.Fatalf("err = %v, want delete error", err)
	}
	if !slices.Equal(client.calls, []string{"delete", "unfence"}) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{"delete", "unfence"})
	}
}

func TestReapTimedOutMigrationTasks(t *testing.T) {
	t.Parallel()

	now := time.Unix(10, 0)
	state := NewControlStateWithClock("test", func() time.Time { return now })
	cfg := MigrationConfig{
		TaskTimeout:  10 * time.Second,
		RetryBackoff: 2 * time.Second,
	}
	seedSingleMigrationTask(t, state)

	claimed := state.ClaimMigrationTask(now)
	if !claimed.Claimed {
		t.Fatal("expected claim")
	}
	svc := newMigrationTestService(state)

	requireTaskNotReapedBeforeTimeout(t, state, svc, cfg, now)

	reapTimedOutMigrationTasks(context.Background(), slog.New(slog.DiscardHandler), svc, cfg, now.Add(11*time.Second))
	requireTaskReapedAfterTimeout(t, state, cfg, now, claimed.Task)
}

func requireTaskNotReapedBeforeTimeout(
	t *testing.T,
	state *ControlState,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	now time.Time,
) {
	t.Helper()

	reapTimedOutMigrationTasks(context.Background(), slog.New(slog.DiscardHandler), svc, cfg, now.Add(5*time.Second))
	if stale := state.TimedOutMigrationTasks(now.Add(5*time.Second), cfg.TaskTimeout); len(stale) != 0 {
		t.Fatalf("timed-out tasks = %#v, want none before timeout", stale)
	}

	running := state.ClaimMigrationTask(now.Add(5 * time.Second))
	if running.Claimed {
		t.Fatalf("expected no claim while task is still running before timeout, got %+v", running)
	}
}

func requireTaskReapedAfterTimeout(
	t *testing.T,
	state *ControlState,
	cfg MigrationConfig,
	now time.Time,
	task controlapi.MigrationTaskBody,
) {
	t.Helper()

	plans := state.MigrationPlans().Plans
	if len(plans) != 1 || len(plans[0].Tasks) != 1 {
		t.Fatalf("migration plans = %+v, want one plan with one task", plans)
	}
	reaped := plans[0].Tasks[0]
	if reaped.TaskID != task.TaskID || reaped.PlanID != task.PlanID {
		t.Fatalf("reaped task = %+v, want %v/%v", reaped, task.PlanID, task.TaskID)
	}
	if reaped.State != "failed" {
		t.Fatalf("task state = %s, want failed", reaped.State)
	}
	if reaped.Attempts != 1 {
		t.Fatalf("task attempts = %d, want 1", reaped.Attempts)
	}
	if reaped.Error != "migration task timed out" {
		t.Fatalf("task error = %q, want migration task timed out", reaped.Error)
	}
	if reaped.NextRetryUnix <= reaped.StartedAtUnix {
		t.Fatalf("next retry unix = %d, want after started_at_unix", reaped.NextRetryUnix)
	}
	if stale := state.TimedOutMigrationTasks(now.Add(11*time.Second), cfg.TaskTimeout); len(stale) != 0 {
		t.Fatalf("timed-out tasks = %#v, want none after task failed", stale)
	}
}

func TestExecutePendingMigrationTasksRetriesAfterFailure(t *testing.T) {
	t.Parallel()

	state := NewControlState("retry")
	now := time.Unix(10, 0)
	svc := newMigrationTestServiceWithClock(state, func() time.Time { return now })
	cfg := MigrationConfig{
		Enabled:       true,
		TaskTimeout:   10 * time.Second,
		RetryBackoff:  20 * time.Millisecond,
		SweepInterval: 10 * time.Millisecond,
	}
	seedSingleMigrationTask(t, state)

	client := &fakeMigrationRangeClient{
		fenceErrs:       []error{errors.New("temporary fence outage")},
		importResponses: []cachewire.MigrationImportResponse{{Imported: 11}},
		exportSnapshots: []cachewire.MigrationSnapshot{{}},
		importErrs:      nil,
		exportErrs:      nil,
		unfenceErrs:     nil,
		deleteErrs:      nil,
	}

	executePendingMigrationTasks(t.Context(), slog.New(slog.DiscardHandler), svc, cfg, client)
	requireSingleMigrationTaskFailed(t, state)

	now = now.Add(1100 * time.Millisecond)
	executePendingMigrationTasks(t.Context(), slog.New(slog.DiscardHandler), svc, cfg, client)
	requireSingleMigrationTaskDoneAfterRetry(t, state)
	requireRetryMigrationCalls(t, client)
}

func requireSingleMigrationTaskFailed(t *testing.T, state *ControlState) {
	t.Helper()

	plans := state.MigrationPlans().Plans
	if len(plans) != 1 {
		t.Fatalf("migration plans = %#v, want one plan", plans)
	}
	failed := plans[0].Tasks[0]
	if failed.State != "failed" {
		t.Fatalf("task state = %s, want failed", failed.State)
	}
	if failed.Attempts != 1 {
		t.Fatalf("task attempts = %d, want 1", failed.Attempts)
	}
}

func requireSingleMigrationTaskDoneAfterRetry(t *testing.T, state *ControlState) {
	t.Helper()

	plans := state.MigrationPlans().Plans
	if len(plans) != 1 {
		t.Fatalf("migration plans = %#v, want one plan after retry", plans)
	}
	done := plans[0].Tasks[0]
	if done.State != "done" {
		t.Fatalf("task state = %s, want done", done.State)
	}
	if done.Attempts != 1 {
		t.Fatalf("task attempts = %d, want 1", done.Attempts)
	}
	if done.ImportedEntries != 11 {
		t.Fatalf("task imported = %d, want 11", done.ImportedEntries)
	}
	if done.DeletedEntries != 3 {
		t.Fatalf("task deleted = %d, want 3", done.DeletedEntries)
	}
}

func requireRetryMigrationCalls(t *testing.T, client *fakeMigrationRangeClient) {
	t.Helper()

	if !slices.Equal(
		client.calls,
		[]string{
			"fence",
			"fence",
			"export",
			"import",
			"delete",
			"unfence",
		},
	) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{
			"fence",
			"fence",
			"export",
			"import",
			"delete",
			"unfence",
		})
	}
}

func seedSingleMigrationTask(t *testing.T, state *ControlState) {
	t.Helper()

	if _, err := state.RegisterNode(context.Background(), "node-1", "127.0.0.1:7403"); err != nil {
		t.Fatalf("register source node: %v", err)
	}
	if _, err := state.CreateNamespace("orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := state.CreateSpace(context.Background(), "orders", "session"); err != nil {
		t.Fatalf("create space: %v", err)
	}
	if _, err := state.RegisterNode(context.Background(), "node-2", "127.0.0.1:7503"); err != nil {
		t.Fatalf("register target node: %v", err)
	}
}
