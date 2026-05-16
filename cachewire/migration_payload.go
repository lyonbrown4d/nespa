package cachewire

func PackMigrationSnapshot(snapshot MigrationSnapshot) (MigrationSnapshot, []byte, error) {
	entries := make([]MigrationSnapshotEntry, 0, len(snapshot.Entries))
	payload := make([]byte, 0, migrationSnapshotPayloadSize(snapshot.Entries))
	for index := range snapshot.Entries {
		entry := snapshot.Entries[index]
		offset, size, err := checkedPayloadRange(len(payload), len(entry.Value))
		if err != nil {
			return MigrationSnapshot{}, nil, err
		}
		entry.PayloadOffset = offset
		entry.PayloadSize = size
		payload = append(payload, entry.Value...)
		entry.Value = nil
		entries = append(entries, entry)
	}
	return MigrationSnapshot{Entries: entries}, payload, nil
}

func UnpackMigrationSnapshot(snapshot MigrationSnapshot, payload []byte) (MigrationSnapshot, error) {
	entries := make([]MigrationSnapshotEntry, 0, len(snapshot.Entries))
	for index := range snapshot.Entries {
		entry := snapshot.Entries[index]
		value, err := SlicePayload(payload, entry.PayloadOffset, entry.PayloadSize)
		if err != nil {
			return MigrationSnapshot{}, err
		}
		entry.Value = value
		entries = append(entries, entry)
	}
	return MigrationSnapshot{Entries: entries}, nil
}

func migrationSnapshotPayloadSize(entries []MigrationSnapshotEntry) int {
	var total int
	for index := range entries {
		total += len(entries[index].Value)
	}
	return total
}

func migrationSnapshotEntryCount(entries []MigrationSnapshotEntry) uint64 {
	var count uint64
	for range entries {
		count++
	}
	return count
}
