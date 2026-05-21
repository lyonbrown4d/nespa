package tcp

import "time"

func (d *replicationDispatcher) Stats() ReplicationStats {
	d.statsMu.RLock()
	stats := d.stats
	d.statsMu.RUnlock()
	stats.QueueDepth = uint64(len(d.jobs))
	stats.QueueCapacity = uint64(cap(d.jobs))
	return stats
}

func (d *replicationDispatcher) recordEnqueued(sequence uint64) {
	d.statsMu.Lock()
	d.stats.Enqueued++
	d.stats.LastQueuedSequence = sequence
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordDropped(sequence uint64) {
	d.statsMu.Lock()
	d.stats.Dropped++
	d.stats.LastDroppedSequence = sequence
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordAttempt(job replicationJob) {
	d.statsMu.Lock()
	d.stats.Attempts++
	d.stats.ActiveTarget = job.target
	d.stats.LastAttemptSequence = job.sequence
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordSuccess(job replicationJob) {
	d.statsMu.Lock()
	d.stats.Successes++
	d.stats.Retrying = false
	d.stats.ActiveTarget = ""
	d.stats.LastSuccessSequence = job.sequence
	d.stats.LastSuccessUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordFailure(job replicationJob, err error) {
	d.statsMu.Lock()
	d.stats.Failures++
	d.stats.Retrying = true
	d.stats.ActiveTarget = job.target
	d.stats.LastFailureSequence = job.sequence
	d.stats.LastError = err.Error()
	d.stats.LastErrorUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) appendOutbox(sequence uint64, target string, command replicationCommand) {
	if d.outbox == nil {
		return
	}
	entry, err := newReplicationOutboxEntry(sequence, target, command)
	if err != nil {
		d.recordOutboxFailure(sequence, err)
		return
	}
	if err := d.outbox.Append(entry); err != nil {
		d.recordOutboxFailure(sequence, err)
		return
	}
	d.recordOutboxAppended(sequence)
}

func (d *replicationDispatcher) recordOutboxAppended(sequence uint64) {
	d.statsMu.Lock()
	d.stats.OutboxEntries++
	d.stats.LastOutboxSequence = sequence
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordOutboxFailure(sequence uint64, err error) {
	d.statsMu.Lock()
	d.stats.OutboxFailures++
	d.stats.LastOutboxSequence = sequence
	d.stats.LastOutboxError = err.Error()
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) restoreOutboxSnapshot(snapshot replicationOutboxSnapshot) {
	d.statsMu.Lock()
	d.stats.OutboxEntries = snapshot.entries
	d.stats.LastOutboxSequence = snapshot.maxSequence
	if snapshot.maxSequence > d.nextSequence {
		d.nextSequence = snapshot.maxSequence
	}
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) ackSuccess(job replicationJob) {
	if d.acks == nil {
		return
	}
	if err := d.acks.Ack(job.target, job.sequence); err != nil {
		d.recordAckFailure(job, err)
		return
	}
	d.recordAckSuccess(job, d.acks.Snapshot())
}

func (d *replicationDispatcher) recordAckSuccess(job replicationJob, snapshot replicationAckSnapshot) {
	d.statsMu.Lock()
	d.stats.LastAckTarget = job.target
	d.stats.LastAckSequence = job.sequence
	d.restoreAckSnapshotLocked(snapshot)
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) recordAckFailure(job replicationJob, err error) {
	d.statsMu.Lock()
	d.stats.AckFailures++
	d.stats.LastAckTarget = job.target
	d.stats.LastAckSequence = job.sequence
	d.stats.LastAckError = err.Error()
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) restoreAckSnapshot(snapshot replicationAckSnapshot) {
	d.statsMu.Lock()
	d.restoreAckSnapshotLocked(snapshot)
	d.statsMu.Unlock()
}

func (d *replicationDispatcher) restoreAckSnapshotLocked(snapshot replicationAckSnapshot) {
	d.stats.AckTargets = snapshot.targets
	if snapshot.maxSequence > d.stats.LastAckSequence {
		d.stats.LastAckSequence = snapshot.maxSequence
	}
}

func (d *replicationDispatcher) nextReplicationSequence() uint64 {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	d.nextSequence++
	return d.nextSequence
}
