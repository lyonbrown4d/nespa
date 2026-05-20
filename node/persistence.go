package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func RestoreEngineSnapshot(ctx context.Context, logger *slog.Logger, cfg Config, eng engine.Engine) error {
	path := strings.TrimSpace(cfg.SnapshotPath)
	if path == "" {
		return nil
	}
	snapshot, err := engine.LoadSnapshotFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("load node engine snapshot: %w", err)
	}
	if err := eng.Restore(ctx, snapshot); err != nil {
		return fmt.Errorf("restore node engine snapshot: %w", err)
	}
	logger.Info("node engine snapshot restored", "path", path, "entries", len(snapshot.Entries))
	return nil
}

func SaveEngineSnapshot(ctx context.Context, logger *slog.Logger, cfg Config, eng engine.Engine) error {
	path := strings.TrimSpace(cfg.SnapshotPath)
	if path == "" {
		return nil
	}
	snapshot, err := eng.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot node engine: %w", err)
	}
	if err := engine.SaveSnapshotFile(path, snapshot); err != nil {
		return fmt.Errorf("save node engine snapshot: %w", err)
	}
	logger.Info("node engine snapshot saved", "path", path, "entries", len(snapshot.Entries))
	return nil
}

func RunSnapshotScheduler(ctx context.Context, logger *slog.Logger, cfg Config, eng engine.Engine) {
	runSnapshotSchedulerWithFunc(ctx, logger, cfg.SnapshotInterval, func(context.Context) error {
		return SaveEngineSnapshot(ctx, logger, cfg, eng)
	})
}

func RunSnapshotSchedulerWithFunc(ctx context.Context, logger *slog.Logger, interval time.Duration, saveFunc func(context.Context) error) {
	runSnapshotSchedulerWithFunc(ctx, logger, interval, saveFunc)
}

func runSnapshotSchedulerWithFunc(ctx context.Context, logger *slog.Logger, interval time.Duration, saveFunc func(context.Context) error) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("node engine snapshot scheduler started", "interval", interval)

	for {
		select {
		case <-ctx.Done():
			logger.Info("node engine snapshot scheduler stopped")
			return
		case now := <-ticker.C:
			if err := saveFunc(ctx); err != nil {
				logger.Warn("node engine snapshot save failed", "at", now, "error", err)
			}
		}
	}
}
