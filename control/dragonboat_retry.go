package control

import (
	"context"
	"errors"
	"time"

	dragonboat "github.com/lni/dragonboat/v3"
	dragonstatemachine "github.com/lni/dragonboat/v3/statemachine"
	"github.com/samber/oops"
)

type dragonboatRawError struct {
	err error
}

func (r *DragonboatRuntime) syncPropose(ctx context.Context, raw []byte) (dragonstatemachine.Result, error) {
	return dragonboatRetry(ctx, r.proposalTimeout, func(attemptCtx context.Context) (dragonstatemachine.Result, error) {
		result, err := r.nodeHost.SyncPropose(attemptCtx, r.session, raw)
		return result, wrapDragonboatRawError(err)
	})
}

func (r *DragonboatRuntime) syncRead(ctx context.Context) (any, error) {
	return dragonboatRetry(ctx, r.proposalTimeout, func(attemptCtx context.Context) (any, error) {
		result, err := r.nodeHost.SyncRead(attemptCtx, r.clusterID, nil)
		return result, wrapDragonboatRawError(err)
	})
}

func dragonboatRetry[T any](
	ctx context.Context,
	timeout time.Duration,
	operation func(context.Context) (T, error),
) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	for {
		attemptCtx, cancel, err := dragonboatAttemptContext(ctx, deadline)
		if err != nil {
			return zero, err
		}
		result, err := operation(attemptCtx)
		cancel()
		if err == nil {
			return result, nil
		}

		rawErr := dragonboatRawErrorValue(err)
		if !dragonboat.IsTempError(rawErr) {
			return zero, wrapDragonboatOperationError(rawErr)
		}
		if err := waitDragonboatRetry(ctx, deadline, rawErr); err != nil {
			return zero, err
		}
	}
}

func dragonboatAttemptContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, nil, wrapDragonboatOperationError(dragonboat.ErrTimeout)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, minDuration(remaining, 200*time.Millisecond))
	return attemptCtx, cancel, nil
}

func waitDragonboatRetry(ctx context.Context, deadline time.Time, lastErr error) error {
	wait := minDuration(time.Until(deadline), 25*time.Millisecond)
	if wait <= 0 {
		return wrapDragonboatOperationError(lastErr)
	}

	timer := time.NewTimer(wait)
	select {
	case <-ctx.Done():
		stopTimer(timer)
		return wrapDragonboatOperationError(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func wrapDragonboatRawError(err error) error {
	if err == nil {
		return nil
	}
	return &dragonboatRawError{err: err}
}

func (e *dragonboatRawError) Error() string {
	return e.err.Error()
}

func (e *dragonboatRawError) Unwrap() error {
	return e.err
}

func dragonboatRawErrorValue(err error) error {
	var raw *dragonboatRawError
	if errors.As(err, &raw) {
		return raw.err
	}
	return err
}

func wrapDragonboatOperationError(err error) error {
	return oops.Code("control_raft_operation_failed").
		In("control.raft").
		Wrapf(err, "run control dragonboat operation")
}
