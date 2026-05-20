package control

import (
	"context"
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
)

type MigrationConfig struct {
	Enabled       bool
	SweepInterval time.Duration
	TaskTimeout   time.Duration
	RetryBackoff  time.Duration
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
	for {
		result, err := svc.claimMigrationTask(ctx)
		if err != nil {
			logger.Warn("control migration task claim failed", "error", err)
			return
		}
		if !result.Claimed {
			return
		}
		executeClaimedMigrationTask(ctx, logger, svc, cfg, client, result.Task)
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
		deleted, err := deleteMigrationSource(taskCtx, client, task, request)
		if err != nil {
			return task.ImportedEntries, 0, err
		}
		if err := unfenceMigrationSource(taskCtx, client, task, request); err != nil {
			return task.ImportedEntries, deleted, fmt.Errorf("unfence migration source: %w", err)
		}
		return task.ImportedEntries, deleted, nil
	}
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
	if err := svc.cutoverMigrationTask(ctx, task, imported.Imported); err != nil {
		return imported.Imported, 0, fmt.Errorf("cutover migration task: %w", err)
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
	return err
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
