package control

import (
	"fmt"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/samber/oops"
)

const (
	migrationTaskPlanned = "planned"
	migrationTaskRunning = "running"
	migrationTaskDone    = "done"
	migrationTaskFailed  = "failed"
)

var ErrMigrationTaskNotFound = oops.Code("migration_task_not_found").
	In("control.migration").
	New("control: migration task not found")

type MigrationTaskResult struct {
	Claimed bool                         `json:"claimed"`
	Task    controlapi.MigrationTaskBody `json:"task"`
}

func (s *ControlState) ClaimMigrationTask(now time.Time) MigrationTaskResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	plans := s.plans.Values()
	for planIndex := range plans {
		if !claimableMigrationPlan(plans[planIndex]) {
			continue
		}
		taskIndex, ok := firstTaskWithState(plans[planIndex], migrationTaskPlanned)
		if !ok {
			continue
		}
		task := markTaskRunning(plans[planIndex].Tasks[taskIndex], now)
		plans[planIndex].Tasks[taskIndex] = task
		plans[planIndex].State = deriveMigrationPlanState(plans[planIndex])
		s.replacePlansLocked(plans)
		s.revision++
		return MigrationTaskResult{Claimed: true, Task: task}
	}
	return MigrationTaskResult{}
}

func claimableMigrationPlan(plan controlapi.MigrationPlanBody) bool {
	return plan.State == migrationTaskPlanned || plan.State == migrationTaskRunning
}

func (s *ControlState) CompleteMigrationTask(
	planID, taskID, imported, deleted uint64,
	now time.Time,
) (controlapi.MigrationTaskBody, error) {
	return s.updateMigrationTask(planID, taskID, func(task controlapi.MigrationTaskBody) controlapi.MigrationTaskBody {
		task.State = migrationTaskDone
		task.Error = ""
		task.ImportedEntries = imported
		task.DeletedEntries = deleted
		task.FinishedAtUnix = now.Unix()
		return task
	})
}

func (s *ControlState) FailMigrationTask(
	planID, taskID uint64,
	message string,
	now time.Time,
) (controlapi.MigrationTaskBody, error) {
	return s.updateMigrationTask(planID, taskID, func(task controlapi.MigrationTaskBody) controlapi.MigrationTaskBody {
		task.State = migrationTaskFailed
		task.Error = message
		task.FinishedAtUnix = now.Unix()
		return task
	})
}

func (s *ControlState) updateMigrationTask(
	planID, taskID uint64,
	mutate func(controlapi.MigrationTaskBody) controlapi.MigrationTaskBody,
) (controlapi.MigrationTaskBody, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plans := s.plans.Values()
	for planIndex := range plans {
		if plans[planIndex].ID != planID {
			continue
		}
		for taskIndex := range plans[planIndex].Tasks {
			if plans[planIndex].Tasks[taskIndex].TaskID != taskID {
				continue
			}
			task := mutate(plans[planIndex].Tasks[taskIndex])
			plans[planIndex].Tasks[taskIndex] = task
			plans[planIndex].State = deriveMigrationPlanState(plans[planIndex])
			s.replacePlansLocked(plans)
			s.revision++
			return task, nil
		}
	}
	return controlapi.MigrationTaskBody{}, migrationTaskNotFound(planID, taskID)
}

func (s *ControlState) replacePlansLocked(plans []controlapi.MigrationPlanBody) {
	s.plans = collectionlist.NewList[controlapi.MigrationPlanBody](plans...)
}

func firstTaskWithState(plan controlapi.MigrationPlanBody, state string) (int, bool) {
	for index := range plan.Tasks {
		if plan.Tasks[index].State == state {
			return index, true
		}
	}
	return 0, false
}

func markTaskRunning(task controlapi.MigrationTaskBody, now time.Time) controlapi.MigrationTaskBody {
	task.State = migrationTaskRunning
	task.Error = ""
	if task.StartedAtUnix == 0 {
		task.StartedAtUnix = now.Unix()
	}
	return task
}

func deriveMigrationPlanState(plan controlapi.MigrationPlanBody) string {
	hasRunning := false
	hasPlanned := false
	for index := range plan.Tasks {
		switch plan.Tasks[index].State {
		case migrationTaskFailed:
			return migrationTaskFailed
		case migrationTaskRunning:
			hasRunning = true
		case migrationTaskPlanned:
			hasPlanned = true
		case migrationTaskDone:
		}
	}
	if hasRunning || hasPlanned {
		return migrationTaskRunning
	}
	return migrationTaskDone
}

func migrationTaskNotFound(planID, taskID uint64) error {
	return fmt.Errorf("%w: plan=%d task=%d", ErrMigrationTaskNotFound, planID, taskID)
}
