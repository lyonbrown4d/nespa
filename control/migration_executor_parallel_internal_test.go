package control

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type blockingExportMigrationClient struct {
	mu            sync.Mutex
	activeExports int
	maxExports    int
	targetExports int
	started       chan struct{}
	release       chan struct{}
	startOnce     sync.Once
	releaseOnce   sync.Once
}

func newBlockingExportMigrationClient(targetExports int) *blockingExportMigrationClient {
	return &blockingExportMigrationClient{
		targetExports: targetExports,
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (f *blockingExportMigrationClient) FenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	return cachewire.MigrationFenceResponse{}, nil
}

func (f *blockingExportMigrationClient) UnfenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	return cachewire.MigrationFenceResponse{}, nil
}

func (f *blockingExportMigrationClient) ExportRange(
	ctx context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationSnapshot, error) {
	f.noteExportStarted()
	select {
	case <-ctx.Done():
		return cachewire.MigrationSnapshot{}, fmt.Errorf("wait for blocked export release: %w", ctx.Err())
	case <-f.release:
	}
	f.noteExportFinished()
	return cachewire.MigrationSnapshot{}, nil
}

func (f *blockingExportMigrationClient) ImportSnapshot(
	_ context.Context, _ string, _ cachewire.MigrationSnapshot,
) (cachewire.MigrationImportResponse, error) {
	return cachewire.MigrationImportResponse{Imported: 1}, nil
}

func (f *blockingExportMigrationClient) DeleteRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationDeleteRangeResponse, error) {
	return cachewire.MigrationDeleteRangeResponse{Deleted: 1}, nil
}

func (f *blockingExportMigrationClient) releaseExports() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *blockingExportMigrationClient) maxActiveExports() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxExports
}

func (f *blockingExportMigrationClient) noteExportStarted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeExports++
	if f.activeExports > f.maxExports {
		f.maxExports = f.activeExports
	}
	if f.activeExports >= f.targetExports {
		f.startOnce.Do(func() { close(f.started) })
	}
}

func (f *blockingExportMigrationClient) noteExportFinished() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeExports--
}

func TestExecutePendingMigrationTasksSerializesSameNodeTasks(t *testing.T) {
	t.Parallel()

	state := NewControlState("parallel")
	svc := &ServiceRuntime{
		state: state,
		fsm:   NewControlFSM(state),
	}
	cfg := MigrationConfig{
		Enabled:          true,
		TaskTimeout:      5 * time.Second,
		RetryBackoff:     time.Second,
		SweepInterval:    10 * time.Millisecond,
		MaxParallelTasks: 2,
	}
	seedParallelMigrationTasks(t, state)

	client := newBlockingExportMigrationClient(1)
	t.Cleanup(client.releaseExports)
	done := make(chan struct{})
	go func() {
		defer close(done)
		executePendingMigrationTasks(t.Context(), slog.New(slog.DiscardHandler), svc, cfg, client)
	}()

	waitForActiveExports(t, client, 1)
	client.releaseExports()
	waitForParallelMigrationExecution(t, done)
	requireMigrationTasksDone(t, state)
}

func TestMigrationTaskWavesSeparateNodeConflicts(t *testing.T) {
	t.Parallel()

	waves := migrationTaskWaves([]controlapi.MigrationTaskBody{
		{TaskID: 1, SourceNodeID: "node-1", TargetNodeID: "node-2"},
		{TaskID: 2, SourceNodeID: "node-3", TargetNodeID: "node-4"},
		{TaskID: 3, SourceNodeID: "node-2", TargetNodeID: "node-5"},
	})

	if len(waves) != 2 {
		t.Fatalf("waves len = %d, want 2: %+v", len(waves), waves)
	}
	requireWaveTasks(t, waves[0], 1, 2)
	requireWaveTasks(t, waves[1], 3)
}

func seedParallelMigrationTasks(t *testing.T, state *ControlState) {
	t.Helper()

	if _, err := state.RegisterNode(context.Background(), "node-1", "127.0.0.1:7403"); err != nil {
		t.Fatalf("register source node: %v", err)
	}
	if _, err := state.CreateNamespace("orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := state.CreateSpace(context.Background(), "orders", "session"); err != nil {
		t.Fatalf("create session space: %v", err)
	}
	if _, err := state.CreateSpace(context.Background(), "orders", "profile"); err != nil {
		t.Fatalf("create profile space: %v", err)
	}
	if _, err := state.RegisterNode(context.Background(), "node-2", "127.0.0.1:7503"); err != nil {
		t.Fatalf("register target node: %v", err)
	}

	plans := state.MigrationPlans().Plans
	if len(plans) != 1 || len(plans[0].Tasks) != 2 {
		t.Fatalf("migration plans = %+v, want one plan with two tasks", plans)
	}
}

func waitForActiveExports(t *testing.T, client *blockingExportMigrationClient, want int) {
	t.Helper()

	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatalf("expected %d active migration exports", want)
	}
	if got := client.maxActiveExports(); got != want {
		t.Fatalf("max active exports = %d, want %d", got, want)
	}
}

func waitForParallelMigrationExecution(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("migration execution did not finish")
	}
}

func requireMigrationTasksDone(t *testing.T, state *ControlState) {
	t.Helper()

	plans := state.MigrationPlans().Plans
	if len(plans) != 1 || len(plans[0].Tasks) != 2 {
		t.Fatalf("migration plans after execution = %+v, want one plan with two tasks", plans)
	}
	for index := range plans[0].Tasks {
		task := plans[0].Tasks[index]
		if task.State != migrationTaskDone {
			t.Fatalf("task %d state = %s, want done", index, task.State)
		}
	}
}

func requireWaveTasks(t *testing.T, tasks []controlapi.MigrationTaskBody, want ...uint64) {
	t.Helper()

	if len(tasks) != len(want) {
		t.Fatalf("wave tasks len = %d, want %d: %+v", len(tasks), len(want), tasks)
	}
	for index := range want {
		if tasks[index].TaskID != want[index] {
			t.Fatalf("wave task %d = %d, want %d", index, tasks[index].TaskID, want[index])
		}
	}
}
