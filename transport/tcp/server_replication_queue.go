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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

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
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := &replicationDispatcher{
		client:            client,
		timeout:           timeout,
		retryInitialDelay: defaultReplicationRetryInitialDelay,
		retryMaxDelay:     defaultReplicationRetryMaxDelay,
		jobs:              make(chan replicationJob, queueSize),
		ctx:               ctx,
		cancel:            cancel,
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

func (d *replicationDispatcher) OpenOutbox(path string) error {
	if !replicationOutboxEnabled(path) {
		return nil
	}
	d.outboxPath = path
	entries, err := scanReplicationOutboxEntries(path)
	if err != nil {
		return err
	}
	snapshot := replayableReplicationOutboxSnapshot(entries)
	outbox, err := openReplicationOutbox(path)
	if err != nil {
		return err
	}
	acks, err := openReplicationAckStore(path)
	if err != nil {
		return err
	}
	d.outbox = outbox
	d.acks = acks
	d.restoreOutboxSnapshot(snapshot)
	d.restoreAckSnapshot(acks.Snapshot())
	d.replayOutbox(entries)
	return nil
}

func (d *replicationDispatcher) Stop(ctx context.Context) error {
	d.cancel()

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
		case <-d.ctx.Done():
			return
		case <-replayTicker.C:
			d.replayOutboxFromDisk()
		case job := <-d.jobs:
			d.send(job)
		}
	}
}

func (d *replicationDispatcher) replayOutboxFromDisk() {
	if len(d.outboxPath) == 0 || d.outbox == nil || d.acks == nil {
		return
	}
	entries, err := scanReplicationOutboxEntries(d.outboxPath)
	if err != nil {
		d.recordReplayError(err)
		return
	}
	d.replayOutbox(entries)
}

func (d *replicationDispatcher) replayOutbox(entries []replicationOutboxEntry) {
	if len(entries) == 0 || d.acks == nil {
		return
	}

	for index := range entries {
		entry := entries[index]
		if entry.Sequence == 0 || entry.Target == "" {
			continue
		}
		if d.shouldReplay(entry) {
			command, err := replicationCommandFromOutboxEntry(entry)
			if err != nil {
				continue
			}
			d.enqueueReplicationJob(replicationJob{
				sequence: entry.Sequence,
				target:   entry.Target,
				command:  command,
			})
		}
	}
}

func (d *replicationDispatcher) shouldReplay(entry replicationOutboxEntry) bool {
	if entry.Sequence == 0 || entry.Target == "" {
		return false
	}
	if acknowledged, ok := d.acks.get(entry.Target); ok && entry.Sequence <= acknowledged {
		return false
	}
	if queued, ok := d.queuedTargetHighWater(entry.Target); ok && entry.Sequence <= queued {
		return false
	}
	return true
}

func (d *replicationDispatcher) queuedTargetHighWater(target string) (uint64, bool) {
	if target == "" {
		return 0, false
	}
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	sequence, ok := d.queued[target]
	return sequence, ok
}

func (d *replicationDispatcher) recordQueued(sequence uint64, target string) {
	if target == "" || sequence == 0 {
		return
	}
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	current, ok := d.queued[target]
	if !ok || sequence > current {
		d.queued[target] = sequence
	}
}

func (d *replicationDispatcher) send(job replicationJob) {
	delay := d.retryInitialDelay
	for {
		ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
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
	case <-d.ctx.Done():
		return
	default:
	}
	select {
	case <-d.ctx.Done():
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
	case <-d.ctx.Done():
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

func (d *replicationDispatcher) recordReplayError(err error) {
	if err == nil {
		return
	}

	d.statsMu.Lock()
	d.stats.LastError = err.Error()
	d.stats.LastErrorUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
}
