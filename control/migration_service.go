package control

import (
	"context"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func (s *ServiceRuntime) claimMigrationTask(ctx context.Context) (MigrationTaskResult, error) {
	result, err := s.apply(ctx, Command{Type: CommandClaimMigrationTask, NowUnix: s.nowUnix()})
	return result.MigrationTask, err
}

func (s *ServiceRuntime) timedOutMigrationTasks(now time.Time, timeout time.Duration) []controlapi.MigrationTaskBody {
	return s.state.TimedOutMigrationTasks(now, timeout)
}

func (s *ServiceRuntime) completeMigrationTask(
	ctx context.Context,
	task controlapi.MigrationTaskBody,
	imported, deleted uint64,
) error {
	_, err := s.apply(ctx, Command{
		Type:            CommandCompleteMigrationTask,
		PlanID:          task.PlanID,
		TaskID:          task.TaskID,
		ImportedEntries: imported,
		DeletedEntries:  deleted,
		NowUnix:         s.nowUnix(),
	})
	return err
}

func (s *ServiceRuntime) failMigrationTask(
	ctx context.Context,
	task controlapi.MigrationTaskBody,
	retryAfter time.Duration,
	cause error,
) error {
	_, err := s.apply(ctx, Command{
		Type:           CommandFailMigrationTask,
		PlanID:         task.PlanID,
		TaskID:         task.TaskID,
		MigrationError: cause.Error(),
		RetryAfterMS:   retryAfter.Milliseconds(),
		NowUnix:        s.nowUnix(),
	})
	return err
}

func (s *ServiceRuntime) cutoverMigrationTask(
	ctx context.Context,
	task controlapi.MigrationTaskBody,
	imported uint64,
) error {
	_, err := s.apply(ctx, Command{
		Type:            CommandCutoverMigrationTask,
		PlanID:          task.PlanID,
		TaskID:          task.TaskID,
		ImportedEntries: imported,
		NowUnix:         s.nowUnix(),
	})
	return err
}
