package cachewire

func appendMigrationSnapshotEntry(raw []byte, entry MigrationSnapshotEntry) []byte {
	raw = appendKey(raw, entry.Key)
	raw = appendUint64(raw, entry.Version)
	raw = appendUint64(raw, entry.NamespaceVersion)
	raw = appendUint64(raw, entry.SpaceVersion)
	raw = appendInt64(raw, entry.ExpireAtUnixMs)
	raw = appendInt64(raw, entry.CreatedAtUnixMs)
	raw = appendInt64(raw, entry.UpdatedAtUnixMs)
	raw = appendInt64(raw, entry.LastAccessAtUnixMs)
	raw = appendUint64(raw, entry.AccessCount)
	raw = appendUint32(raw, entry.PayloadOffset)
	return appendUint32(raw, entry.PayloadSize)
}
