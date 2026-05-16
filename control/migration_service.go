package control

import (
	"context"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func (s *ServiceRuntime) claimMigrationTask(ctx context.Context) (MigrationTaskResult, error) {
	result, err := s.apply(ctx, Command{Type: CommandClaimMigrationTask, NowUnix: time.Now().Unix()})
	return result.MigrationTask, err
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
		NowUnix:         time.Now().Unix(),
	})
	return err
}

func (s *ServiceRuntime) failMigrationTask(
	ctx context.Context,
	task controlapi.MigrationTaskBody,
	cause error,
) error {
	_, err := s.apply(ctx, Command{
		Type:           CommandFailMigrationTask,
		PlanID:         task.PlanID,
		TaskID:         task.TaskID,
		MigrationError: cause.Error(),
		NowUnix:        time.Now().Unix(),
	})
	return err
}
