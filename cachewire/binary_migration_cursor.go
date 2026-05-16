package cachewire

func (c *metadataCursor) readMigrationRangeRequest() (MigrationRangeRequest, error) {
	namespace, err := c.readString()
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	space, err := c.readString()
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	start, err := c.readUint32()
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	end, err := c.readUint32()
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	return MigrationRangeRequest{Namespace: namespace, Space: space, VSlotStart: start, VSlotEnd: end}, nil
}

func (c *metadataCursor) readMigrationSnapshotEntries(count int) ([]MigrationSnapshotEntry, error) {
	entries := make([]MigrationSnapshotEntry, 0, count)
	for range count {
		entry, err := c.readMigrationSnapshotEntry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (c *metadataCursor) readMigrationSnapshotEntry() (MigrationSnapshotEntry, error) {
	key, err := c.readKey()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	return c.readMigrationSnapshotEntryBody(key)
}

func (c *metadataCursor) readMigrationSnapshotEntryBody(key Key) (MigrationSnapshotEntry, error) {
	version, err := c.readUint64()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	versions, err := c.readMigrationSnapshotVersions()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	times, err := c.readMigrationSnapshotTimes()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	return c.readMigrationSnapshotTail(key, version, versions, times)
}
