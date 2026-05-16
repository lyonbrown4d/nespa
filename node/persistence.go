package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

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
