package cachewire

type migrationVersions struct {
	namespace uint64
	space     uint64
}

type migrationTimes struct {
	expire     int64
	created    int64
	updated    int64
	lastAccess int64
}

func (c *metadataCursor) readMigrationSnapshotVersions() (migrationVersions, error) {
	namespace, err := c.readUint64()
	if err != nil {
		return migrationVersions{}, err
	}
	space, err := c.readUint64()
	if err != nil {
		return migrationVersions{}, err
	}
	return migrationVersions{namespace: namespace, space: space}, nil
}

func (c *metadataCursor) readMigrationSnapshotTimes() (migrationTimes, error) {
	expire, err := c.readInt64()
	if err != nil {
		return migrationTimes{}, err
	}
	created, err := c.readInt64()
	if err != nil {
		return migrationTimes{}, err
	}
	updated, err := c.readInt64()
	if err != nil {
		return migrationTimes{}, err
	}
	lastAccess, err := c.readInt64()
	if err != nil {
		return migrationTimes{}, err
	}
	return migrationTimes{expire: expire, created: created, updated: updated, lastAccess: lastAccess}, nil
}

func (c *metadataCursor) readMigrationSnapshotTail(
	key Key,
	version uint64,
	versions migrationVersions,
	times migrationTimes,
) (MigrationSnapshotEntry, error) {
	accessCount, err := c.readUint64()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	payloadOffset, err := c.readUint32()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	payloadSize, err := c.readUint32()
	if err != nil {
		return MigrationSnapshotEntry{}, err
	}
	return MigrationSnapshotEntry{
		Key:                key,
		Version:            version,
		NamespaceVersion:   versions.namespace,
		SpaceVersion:       versions.space,
		ExpireAtUnixMs:     times.expire,
		CreatedAtUnixMs:    times.created,
		UpdatedAtUnixMs:    times.updated,
		LastAccessAtUnixMs: times.lastAccess,
		AccessCount:        accessCount,
		PayloadOffset:      payloadOffset,
		PayloadSize:        payloadSize,
	}, nil
}
