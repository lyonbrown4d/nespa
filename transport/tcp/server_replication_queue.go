package tcp

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultReplicationQueueSize         = 1024
	defaultReplicationReplayInterval    = 250 * time.Millisecond
	defaultReplicationRetryInitialDelay = 25 * time.Millisecond
	defaultReplicationRetryMaxDelay     = 500 * time.Millisecond
)

type replicationJob struct {
	sequence uint64
	target   string
	command  replicationCommand
}

type ReplicationStats struct {
	QueueDepth          uint64
	QueueCapacity       uint64
	Enqueued            uint64
	Dropped             uint64
	Attempts            uint64
	Successes           uint64
	Failures            uint64
	OutboxEntries       uint64
	OutboxFailures      uint64
	AckTargets          uint64
	AckFailures         uint64
	LastQueuedSequence  uint64
	LastAttemptSequence uint64
	LastSuccessSequence uint64
	LastFailureSequence uint64
	LastDroppedSequence uint64
	LastOutboxSequence  uint64
	LastAckSequence     uint64
	Retrying            bool
	ActiveTarget        string
	LastAckTarget       string
	LastAckError        string
	LastOutboxError     string
	LastError           string
	LastErrorUnixMs     int64
	LastSuccessUnixMs   int64
}

type replicationDispatcher struct {
	client            *Client
	timeout           time.Duration
	retryInitialDelay time.Duration
	retryMaxDelay     time.Duration
	jobs              chan replicationJob

	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	statsMu sync.RWMutex
	stats   ReplicationStats

	nextSequence uint64
	outbox       *replicationOutbox
	acks         *replicationAckStore
	outboxPath   string
	queued       map[string]uint64
}

func newReplicationDispatcher(client *Client, timeout time.Duration, queueSize int) *replicationDispatcher {
	if timeout <= 0 {
		timeout = defaultReplicationTimeout
	}
	if queueSize <= 0 {
		queueSize = defaultReplicationQueueSize
	}
	dispatcher := &replicationDispatcher{
		client:            client,
		timeout:           timeout,
		retryInitialDelay: defaultReplicationRetryInitialDelay,
		retryMaxDelay:     defaultReplicationRetryMaxDelay,
		jobs:              make(chan replicationJob, queueSize),
		done:              make(chan struct{}),
		queued:            map[string]uint64{},
	}
	dispatcher.wg.Go(dispatcher.run)
	return dispatcher
}

func (d *replicationDispatcher) Enqueue(target string, command replicationCommand) {
	if target == "" || !command.valid() {
		return
	}

	sequence := d.nextReplicationSequence()
	d.appendOutbox(sequence, target, command)
	d.enqueueReplicationJob(replicationJob{
		sequence: sequence,
		target:   target,
		command:  command,
	})
}

func (d *replicationDispatcher) Stop(ctx context.Context) error {
	d.stopOnce.Do(func() {
		close(d.done)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		d.wg.Wait()
	}()

	select {
	case <-done:
		if err := d.outbox.Close(); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop replication dispatcher: %w", ctx.Err())
	}
}

func (d *replicationDispatcher) run() {
	replayTicker := time.NewTicker(defaultReplicationReplayInterval)
	defer replayTicker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-replayTicker.C:
			d.replayOutboxFromDisk()
		case job := <-d.jobs:
			d.send(job)
		}
	}
}

func (d *replicationDispatcher) send(job replicationJob) {
	delay := d.retryInitialDelay
	for {
		ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
		d.recordAttempt(job)
		err := job.command.send(ctx, d.client, job.target)
		cancel()
		if err == nil {
			d.recordSuccess(job)
			d.ackSuccess(job)
			return
		}
		d.recordFailure(job, err)
		if !d.waitRetry(delay) {
			return
		}
		delay = nextReplicationRetryDelay(delay, d.retryMaxDelay)
	}
}

func (d *replicationDispatcher) enqueueReplicationJob(job replicationJob) {
	select {
	case <-d.done:
		return
	default:
	}
	select {
	case <-d.done:
		return
	case d.jobs <- job:
		d.recordEnqueued(job.sequence)
		d.recordQueued(job.sequence, job.target)
	default:
		d.recordDropped(job.sequence)
	}
}

func (d *replicationDispatcher) waitRetry(delay time.Duration) bool {
	if delay <= 0 {
		delay = d.retryInitialDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-d.done:
		return false
	case <-timer.C:
		return true
	}
}

func nextReplicationRetryDelay(current, maxDelay time.Duration) time.Duration {
	if current <= 0 {
		return maxDelay
	}
	next := current * 2
	if next > maxDelay {
		return maxDelay
	}
	return next
}
