package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

const (
	defaultMigrationSweepInterval = time.Second
	defaultMigrationTaskTimeout   = 10 * time.Second
	defaultMigrationRetryBackoff  = 2 * time.Second
	defaultMigrationMaxParallel   = 1
)

type MigrationConfig struct {
	Enabled          bool
	SweepInterval    time.Duration
	TaskTimeout      time.Duration
	RetryBackoff     time.Duration
	MaxParallelTasks int
}

type migrationRangeClient interface {
	FenceRange(context.Context, string, cachewire.MigrationRangeRequest) (cachewire.MigrationFenceResponse, error)
	UnfenceRange(context.Context, string, cachewire.MigrationRangeRequest) (cachewire.MigrationFenceResponse, error)
	ExportRange(context.Context, string, cachewire.MigrationRangeRequest) (cachewire.MigrationSnapshot, error)
	ImportSnapshot(context.Context, string, cachewire.MigrationSnapshot) (cachewire.MigrationImportResponse, error)
	DeleteRange(context.Context, string, cachewire.MigrationRangeRequest) (cachewire.MigrationDeleteRangeResponse, error)
}

func StartMigrationExecutor(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	cfg := normalizeMigrationConfig(svc.migration)
	if !cfg.Enabled {
		return nil
	}
	go runMigrationExecutor(ctx, logger, svc, cfg, cachetcp.NewClient())
	return nil
}

func normalizeMigrationConfig(cfg MigrationConfig) MigrationConfig {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultMigrationSweepInterval
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = defaultMigrationTaskTimeout
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultMigrationRetryBackoff
	}
	if cfg.MaxParallelTasks <= 0 {
		cfg.MaxParallelTasks = defaultMigrationMaxParallel
	}
	return cfg
}

func runMigrationExecutor(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client migrationRangeClient,
) {
	executePendingMigrationTasks(ctx, logger, svc, cfg, client)
	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			executePendingMigrationTasks(ctx, logger, svc, cfg, client)
		}
	}
}

func executePendingMigrationTasks(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client migrationRangeClient,
) {
	reapTimedOutMigrationTasks(ctx, logger, svc, cfg, svc.nowTime())
	for {
		tasks, keepClaiming := claimMigrationTaskBatch(ctx, logger, svc, cfg)
		if len(tasks) == 0 {
			return
		}
		executeClaimedMigrationTaskBatch(ctx, logger, svc, cfg, client, tasks)
		if !keepClaiming || len(tasks) < migrationTaskConcurrency(cfg) {
			return
		}
	}
}

func reapTimedOutMigrationTasks(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	now time.Time,
) {
	staleTasks := svc.timedOutMigrationTasks(now, cfg.TaskTimeout)
	for index := range staleTasks {
		task := staleTasks[index]
		err := svc.failMigrationTask(
			ctx,
			task,
			retryDelay(task, cfg),
			errors.New("migration task timed out"),
		)
		if err != nil {
			logger.Warn(
				"control migration task reap failed",
				"plan_id", task.PlanID,
				"task_id", task.TaskID,
				"error", err,
			)
		}
	}
}

func executeClaimedMigrationTask(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client migrationRangeClient,
	task controlapi.MigrationTaskBody,
) {
	imported, deleted, err := migrateRange(ctx, svc, client, cfg, task)
	if err != nil {
		if failErr := svc.failMigrationTask(ctx, task, retryDelay(task, cfg), err); failErr != nil {
			logger.Warn("control migration task fail mark failed", "error", failErr)
		}
		logger.Warn("control migration task failed", "plan_id", task.PlanID, "task_id", task.TaskID, "error", err)
		return
	}
	if err := svc.completeMigrationTask(ctx, task, imported, deleted); err != nil {
		logger.Warn("control migration task complete mark failed", "error", err)
		return
	}
	logger.Info("control migration task completed",
		"plan_id", task.PlanID,
		"task_id", task.TaskID,
		"imported", imported,
		"deleted", deleted,
	)
}

func migrateRange(
	ctx context.Context,
	svc *ServiceRuntime,
	client migrationRangeClient,
	cfg MigrationConfig,
	task controlapi.MigrationTaskBody,
) (uint64, uint64, error) {
	taskCtx, cancel := context.WithTimeout(ctx, cfg.TaskTimeout)
	defer cancel()

	request := migrationRangeRequest(task)
	if task.CutoverAtUnix > 0 {
		return cleanupMigratedRange(taskCtx, client, task, request)
	}
	return migrateRangeToTarget(ctx, taskCtx, svc, client, task, request)
}

func migrateRangeToTarget(
	ctx context.Context,
	taskCtx context.Context,
	svc *ServiceRuntime,
	client migrationRangeClient,
	task controlapi.MigrationTaskBody,
	request cachewire.MigrationRangeRequest,
) (uint64, uint64, error) {
	if _, err := client.FenceRange(taskCtx, task.SourceAddr, request); err != nil {
		return 0, 0, fmt.Errorf("fence migration range: %w", err)
	}

	snapshot, err := client.ExportRange(taskCtx, task.SourceAddr, request)
	if err != nil {
		return 0, 0, fmt.Errorf("export migration range: %w", err)
	}
	imported, err := client.ImportSnapshot(taskCtx, task.TargetAddr, snapshot)
	if err != nil {
		return 0, 0, fmt.Errorf("import migration snapshot: %w", err)
	}
	if cutoverErr := svc.cutoverMigrationTask(ctx, task, imported.Imported); cutoverErr != nil {
		return imported.Imported, 0, fmt.Errorf("cutover migration task: %w", cutoverErr)
	}
	deleted, err := deleteMigrationSource(taskCtx, client, task, request)
	if err != nil {
		return imported.Imported, 0, err
	}
	if err := unfenceMigrationSource(taskCtx, client, task, request); err != nil {
		return imported.Imported, deleted, fmt.Errorf("unfence migration range: %w", err)
	}
	return imported.Imported, deleted, nil
}

func cleanupMigratedRange(
	ctx context.Context,
	client migrationRangeClient,
	task controlapi.MigrationTaskBody,
	request cachewire.MigrationRangeRequest,
) (uint64, uint64, error) {
	deleted, deleteErr := deleteMigrationSource(ctx, client, task, request)
	unfenceErr := unfenceMigrationSource(ctx, client, task, request)
	return migrationCleanupResult(task.ImportedEntries, deleted, deleteErr, unfenceErr)
}

func migrationCleanupResult(imported, deleted uint64, deleteErr, unfenceErr error) (uint64, uint64, error) {
	if deleteErr != nil && unfenceErr != nil {
		return imported, deleted, errors.Join(
			fmt.Errorf("delete source migration range: %w", deleteErr),
			fmt.Errorf("unfence migration source: %w", unfenceErr),
		)
	}
	if deleteErr != nil {
		return imported, deleted, fmt.Errorf("delete source migration range: %w", deleteErr)
	}
	if unfenceErr != nil {
		return imported, deleted, fmt.Errorf("unfence migration source: %w", unfenceErr)
	}
	return imported, deleted, nil
}

func deleteMigrationSource(
	ctx context.Context,
	client migrationRangeClient,
	task controlapi.MigrationTaskBody,
	request cachewire.MigrationRangeRequest,
) (uint64, error) {
	deleted, err := client.DeleteRange(ctx, task.SourceAddr, request)
	if err != nil {
		return 0, fmt.Errorf("delete source migration range: %w", err)
	}
	return deleted.Deleted, nil
}

func unfenceMigrationSource(
	ctx context.Context,
	client migrationRangeClient,
	task controlapi.MigrationTaskBody,
	request cachewire.MigrationRangeRequest,
) error {
	unfenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultMigrationTaskTimeout)
	defer cancel()
	_, err := client.UnfenceRange(unfenceCtx, task.SourceAddr, request)
	if err != nil {
		return fmt.Errorf("unfence migration source: %w", err)
	}
	return nil
}

func retryDelay(task controlapi.MigrationTaskBody, cfg MigrationConfig) time.Duration {
	delay := cfg.RetryBackoff
	for range task.Attempts {
		delay *= 2
		if delay >= cfg.TaskTimeout {
			return cfg.TaskTimeout
		}
	}
	return delay
}

func migrationRangeRequest(task controlapi.MigrationTaskBody) cachewire.MigrationRangeRequest {
	return cachewire.MigrationRangeRequest{
		Namespace:  task.Namespace,
		Space:      task.Space,
		VSlotStart: task.VSlotStart,
		VSlotEnd:   task.VSlotEnd,
		RouteEpoch: task.Revision,
	}
}
