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
	migrationTaskCleanup = "cleanup"
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
		taskIndex, ok := firstClaimableTask(plans[planIndex], now)
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

func (s *ControlState) TimedOutMigrationTasks(now time.Time, timeout time.Duration) []controlapi.MigrationTaskBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if timeout <= 0 {
		return nil
	}

	plans := s.plans.Values()
	var timedOut []controlapi.MigrationTaskBody
	for planIndex := range plans {
		for taskIndex := range plans[planIndex].Tasks {
			task := plans[planIndex].Tasks[taskIndex]
			if timedOutMigrationTask(task, now, timeout) {
				timedOut = append(timedOut, task)
			}
		}
	}
	return timedOut
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

func (s *ControlState) CutoverMigrationTask(
	planID, taskID, imported uint64,
	now time.Time,
) (controlapi.MigrationTaskBody, error) {
	return s.updateMigrationTask(planID, taskID, func(task controlapi.MigrationTaskBody) controlapi.MigrationTaskBody {
		task.State = migrationTaskCleanup
		task.Error = ""
		task.ImportedEntries = imported
		task.CutoverAtUnix = now.Unix()
		task.NextRetryUnix = 0
		return task
	})
}

func (s *ControlState) FailMigrationTask(
	planID, taskID uint64,
	message string,
	retryAfter time.Duration,
	now time.Time,
) (controlapi.MigrationTaskBody, error) {
	return s.updateMigrationTask(planID, taskID, func(task controlapi.MigrationTaskBody) controlapi.MigrationTaskBody {
		task.State = migrationTaskFailed
		task.Error = message
		task.Attempts++
		task.NextRetryUnix = now.Add(retryAfter).Unix()
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

func firstClaimableTask(plan controlapi.MigrationPlanBody, now time.Time) (int, bool) {
	for index := range plan.Tasks {
		if claimableMigrationTask(plan.Tasks[index], now) {
			return index, true
		}
	}
	return 0, false
}

func claimableMigrationTask(task controlapi.MigrationTaskBody, now time.Time) bool {
	switch task.State {
	case migrationTaskPlanned, migrationTaskCleanup:
		return true
	case migrationTaskFailed:
		return task.NextRetryUnix <= now.Unix()
	case migrationTaskRunning, migrationTaskDone:
		return false
	}
	return false
}

func markTaskRunning(task controlapi.MigrationTaskBody, now time.Time) controlapi.MigrationTaskBody {
	task.State = migrationTaskRunning
	task.Error = ""
	task.NextRetryUnix = 0
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
		case migrationTaskCleanup:
			hasRunning = true
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

func timedOutMigrationTask(task controlapi.MigrationTaskBody, now time.Time, timeout time.Duration) bool {
	switch task.State {
	case migrationTaskRunning, migrationTaskCleanup:
	default:
		return false
	}
	if task.StartedAtUnix == 0 {
		return false
	}
	return now.Sub(time.Unix(task.StartedAtUnix, 0)) >= timeout
}
