package tcp

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultReplicationQueueSize         = 1024
	defaultReplicationRetryInitialDelay = 25 * time.Millisecond
	defaultReplicationRetryMaxDelay     = 500 * time.Millisecond
)

type replicationSend func(context.Context, *Client, string) error

type replicationJob struct {
	target string
	send   replicationSend
}

type ReplicationStats struct {
	QueueDepth        uint64
	QueueCapacity     uint64
	Enqueued          uint64
	Dropped           uint64
	Attempts          uint64
	Successes         uint64
	Failures          uint64
	Retrying          bool
	ActiveTarget      string
	LastError         string
	LastErrorUnixMs   int64
	LastSuccessUnixMs int64
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
	}
	dispatcher.wg.Go(dispatcher.run)
	return dispatcher
}

func (d *replicationDispatcher) Enqueue(target string, send replicationSend) {
	if target == "" || send == nil {
		return
	}
	select {
	case <-d.ctx.Done():
		return
	case d.jobs <- replicationJob{target: target, send: send}:
		d.recordEnqueued()
	default:
		d.recordDropped()
	}
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
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop replication dispatcher: %w", ctx.Err())
	}
}

func (d *replicationDispatcher) run() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case job := <-d.jobs:
			d.send(job)
		}
	}
}

func (d *replicationDispatcher) send(job replicationJob) {
	delay := d.retryInitialDelay
	for {
		ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
		d.recordAttempt(job.target)
		err := job.send(ctx, d.client, job.target)
		cancel()
		if err == nil {
			d.recordSuccess()
			return
		}
		d.recordFailure(job.target, err)
		if !d.waitRetry(delay) {
			return
		}
		delay = nextReplicationRetryDelay(delay, d.retryMaxDelay)
	}
}

func (d *replicationDispatcher) Stats() ReplicationStats {
	d.statsMu.RLock()
	stats := d.stats
	d.statsMu.RUnlock()
	stats.QueueDepth = uint64(len(d.jobs))
	stats.QueueCapacity = uint64(cap(d.jobs))
	return stats
}

func (d *replicationDispatcher) recordEnqueued() {
	d.statsMu.Lock()
	d.stats.Enqueued++
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordDropped() {
	d.statsMu.Lock()
	d.stats.Dropped++
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordAttempt(target string) {
	d.statsMu.Lock()
	d.stats.Attempts++
	d.stats.ActiveTarget = target
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordSuccess() {
	d.statsMu.Lock()
	d.stats.Successes++
	d.stats.Retrying = false
	d.stats.ActiveTarget = ""
	d.stats.LastSuccessUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordFailure(target string, err error) {
	d.statsMu.Lock()
	d.stats.Failures++
	d.stats.Retrying = true
	d.stats.ActiveTarget = target
	d.stats.LastError = err.Error()
	d.stats.LastErrorUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
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
