package control

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type fakeMigrationRangeClient struct {
	fenceErrs       []error
	unfenceErrs     []error
	deleteErrs      []error
	exportErrs      []error
	exportSnapshots []cachewire.MigrationSnapshot
	importErrs      []error
	importResponses []cachewire.MigrationImportResponse
	deleteCount     uint64
	calls           []string
}

func (f *fakeMigrationRangeClient) FenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	f.calls = append(f.calls, "fence")
	return f.popFenceErr()
}

func (f *fakeMigrationRangeClient) UnfenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	f.calls = append(f.calls, "unfence")
	return f.popUnfenceErr()
}

func (f *fakeMigrationRangeClient) ExportRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationSnapshot, error) {
	f.calls = append(f.calls, "export")
	snapshot, remaining := popSnapshot(f.exportSnapshots)
	f.exportSnapshots = remaining
	err, remainingErr := popErr(f.exportErrs)
	f.exportErrs = remainingErr
	return snapshot, err
}

func (f *fakeMigrationRangeClient) ImportSnapshot(
	_ context.Context, _ string, _ cachewire.MigrationSnapshot,
) (cachewire.MigrationImportResponse, error) {
	f.calls = append(f.calls, "import")
	response, remaining := popImportResponse(f.importResponses)
	f.importResponses = remaining
	err, remainingErr := popErr(f.importErrs)
	f.importErrs = remainingErr
	if err != nil {
		return response, err
	}
	return response, nil
}

func (f *fakeMigrationRangeClient) DeleteRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationDeleteRangeResponse, error) {
	f.calls = append(f.calls, "delete")
	f.deleteCount++
	err := f.nextDeleteErr()
	if err != nil {
		return cachewire.MigrationDeleteRangeResponse{}, err
	}
	return cachewire.MigrationDeleteRangeResponse{Deleted: 3}, nil
}

func (f *fakeMigrationRangeClient) popFenceErr() (cachewire.MigrationFenceResponse, error) {
	err, remaining := popErr(f.fenceErrs)
	f.fenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) popUnfenceErr() (cachewire.MigrationFenceResponse, error) {
	err, remaining := popErr(f.unfenceErrs)
	f.unfenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) nextDeleteErr() error {
	err, remaining := popErr(f.deleteErrs)
	f.deleteErrs = remaining
	return err
}

func popErr(errs []error) (error, []error) {
	if len(errs) == 0 {
		return nil, nil
	}
	return errs[0], errs[1:]
}

func popSnapshot(snapshots []cachewire.MigrationSnapshot) (cachewire.MigrationSnapshot, []cachewire.MigrationSnapshot) {
	if len(snapshots) == 0 {
		return cachewire.MigrationSnapshot{}, nil
	}
	return snapshots[0], snapshots[1:]
}

func popImportResponse(
	responses []cachewire.MigrationImportResponse,
) (cachewire.MigrationImportResponse, []cachewire.MigrationImportResponse) {
	if len(responses) == 0 {
		return cachewire.MigrationImportResponse{Imported: 0}, responses
	}
	return responses[0], responses[1:]
}

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

	if _, err := state.RegisterNode(context.Background(), "node-1", "127.0.0.1:7403"); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := state.CreateNamespace("orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := state.CreateSpace(context.Background(), "orders", "session"); err != nil {
		t.Fatalf("create space: %v", err)
	}
	if _, err := state.RegisterNode(context.Background(), "node-2", "127.0.0.1:7503"); err != nil {
		t.Fatalf("register node: %v", err)
	}

	claimed := state.ClaimMigrationTask(now)
	if !claimed.Claimed {
		t.Fatal("expected claim")
	}
	task := claimed.Task

	svc := &ServiceRuntime{
		state: state,
		fsm:   NewControlFSM(state),
	}

	// before timeout: task is still running and should not be reaped.
	reapTimedOutMigrationTasks(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), svc, cfg, now.Add(5*time.Second))
	if stale := state.TimedOutMigrationTasks(now.Add(5*time.Second), cfg.TaskTimeout); len(stale) != 0 {
		t.Fatalf("timed-out tasks = %#v, want none before timeout", stale)
	}

	running := state.ClaimMigrationTask(now.Add(5 * time.Second))
	if running.Claimed {
		t.Fatalf("expected no claim while task is still running before timeout, got %+v", running)
	}

	// at timeout boundary: task should be moved to failed state and retriable.
	reapTimedOutMigrationTasks(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), svc, cfg, now.Add(11*time.Second))
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
	svc := &ServiceRuntime{
		state: state,
		fsm:   NewControlFSM(state),
		now: func() time.Time {
			return now
		},
	}
	cfg := MigrationConfig{
		Enabled:       true,
		TaskTimeout:   10 * time.Second,
		RetryBackoff:  20 * time.Millisecond,
		SweepInterval: 10 * time.Millisecond,
	}
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

	client := &fakeMigrationRangeClient{
		fenceErrs:       []error{errors.New("temporary fence outage")},
		importResponses: []cachewire.MigrationImportResponse{{Imported: 11}},
		exportSnapshots: []cachewire.MigrationSnapshot{{}},
		importErrs:      nil,
		exportErrs:      nil,
		unfenceErrs:     nil,
		deleteErrs:      nil,
	}

	executePendingMigrationTasks(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), svc, cfg, client)

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

	now = now.Add(1100 * time.Millisecond)
	executePendingMigrationTasks(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), svc, cfg, client)

	plans = state.MigrationPlans().Plans
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

func TestMigrationTaskNextRetryUnix(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)

	if got := migrationTaskNextRetryUnix(now, -1*time.Second); got != now.Unix() {
		t.Fatalf("retry after -1s = %d, want %d", got, now.Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 0); got != now.Unix() {
		t.Fatalf("retry after 0s = %d, want %d", got, now.Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 300*time.Millisecond); got != now.Add(time.Second).Unix() {
		t.Fatalf("retry after 300ms = %d, want %d", got, now.Add(time.Second).Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 2*time.Second); got != now.Add(2*time.Second).Unix() {
		t.Fatalf("retry after 2s = %d, want %d", got, now.Add(2*time.Second).Unix())
	}
}
