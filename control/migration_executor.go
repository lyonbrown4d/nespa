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
)

type MigrationConfig struct {
	Enabled       bool
	SweepInterval time.Duration
	TaskTimeout   time.Duration
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
	return cfg
}

func runMigrationExecutor(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client *cachetcp.Client,
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
	client *cachetcp.Client,
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
	client *cachetcp.Client,
	task controlapi.MigrationTaskBody,
) {
	imported, deleted, err := migrateRange(ctx, client, cfg.TaskTimeout, task)
	if err != nil {
		if failErr := svc.failMigrationTask(ctx, task, err); failErr != nil {
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
	client *cachetcp.Client,
	timeout time.Duration,
	task controlapi.MigrationTaskBody,
) (uint64, uint64, error) {
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request := migrationRangeRequest(task)
	snapshot, err := client.ExportRange(taskCtx, task.SourceAddr, request)
	if err != nil {
		return 0, 0, fmt.Errorf("export migration range: %w", err)
	}
	imported, err := client.ImportSnapshot(taskCtx, task.TargetAddr, snapshot)
	if err != nil {
		return 0, 0, fmt.Errorf("import migration snapshot: %w", err)
	}
	deleted, err := client.DeleteRange(taskCtx, task.SourceAddr, request)
	if err != nil {
		return imported.Imported, 0, fmt.Errorf("delete source migration range: %w", err)
	}
	return imported.Imported, deleted.Deleted, nil
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
