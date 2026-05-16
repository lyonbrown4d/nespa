package control

import (
	"context"
	"log/slog"
	"time"
)

func StartLiveness(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime) error {
	go runLivenessSweep(ctx, logger, svc, svc.liveness)
	return nil
}

func normalizeLivenessConfig(cfg LivenessConfig) LivenessConfig {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Second
	}
	if cfg.SuspectAfter <= 0 {
		cfg.SuspectAfter = 15 * time.Second
	}
	if cfg.DeadAfter <= 0 {
		cfg.DeadAfter = 30 * time.Second
	}
	if cfg.DeadAfter < cfg.SuspectAfter {
		cfg.DeadAfter = cfg.SuspectAfter
	}
	return cfg
}

func runLivenessSweep(ctx context.Context, logger *slog.Logger, svc *ServiceRuntime, cfg LivenessConfig) {
	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			result, err := svc.advanceLiveness(ctx, now, cfg.SuspectAfter, cfg.DeadAfter)
			if err != nil {
				logger.Warn("control node liveness sweep failed", "error", err)
				continue
			}
			for _, node := range result.Changed {
				logger.Warn("control node liveness changed", "node_id", node.NodeID, "state", node.State, "revision", result.Revision)
			}
		}
	}
}

func (s *ServiceRuntime) advanceLiveness(ctx context.Context, now time.Time, suspectAfter, deadAfter time.Duration) (LivenessResult, error) {
	result, err := s.apply(ctx, Command{
		Type:           CommandAdvanceNodeLiveness,
		NowUnix:        now.Unix(),
		SuspectAfterMS: suspectAfter.Milliseconds(),
		DeadAfterMS:    deadAfter.Milliseconds(),
	})
	return result.Liveness, err
}
