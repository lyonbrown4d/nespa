package engine

import (
	"context"
	"errors"
	"time"

	"github.com/arcgolabs/dix"
)

func Module(eng Engine, sweepInterval time.Duration) dix.Module {
	if sweepInterval <= 0 {
		sweepInterval = time.Second
	}

	return dix.NewModule("node.engine",
		dix.WithModuleProviders(
			dix.Value[Engine](eng),
		),
		dix.WithModuleHooks(
			dix.OnStart[Engine](func(ctx context.Context, eng Engine) error {
				go runSweeper(ctx, eng, sweepInterval)
				return nil
			}, dix.LifecycleName("node.engine.sweeper.start")),
			dix.OnStop[Engine](func(_ context.Context, eng Engine) error {
				return eng.Close()
			}, dix.LifecycleName("node.engine.stop")),
		),
	)
}

func runSweeper(ctx context.Context, eng Engine, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, err := eng.SweepExpired(ctx, now)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
				return
			}
		}
	}
}
