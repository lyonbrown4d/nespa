package tcp

import "time"

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

func (d *replicationDispatcher) replayOutboxFromDisk() {
	if d.outboxPath == "" || d.outbox == nil || d.acks == nil {
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
		if d.shouldReplay(entry) {
			d.enqueueReplayEntry(entry)
		}
	}
}

func (d *replicationDispatcher) enqueueReplayEntry(entry replicationOutboxEntry) {
	command, err := replicationCommandFromOutboxEntry(entry)
	if err != nil {
		d.recordReplayError(err)
		return
	}
	d.enqueueReplicationJob(replicationJob{
		sequence: entry.Sequence,
		target:   entry.Target,
		command:  command,
	})
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

func (d *replicationDispatcher) recordReplayError(err error) {
	if err == nil {
		return
	}

	d.statsMu.Lock()
	d.stats.LastError = err.Error()
	d.stats.LastErrorUnixMs = time.Now().UnixMilli()
	d.statsMu.Unlock()
}
