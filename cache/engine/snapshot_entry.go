package engine

func entryFromSnapshot(item SnapshotEntry) (*entry, error) {
	if err := validateKey(item.Key); err != nil {
		return nil, err
	}
	ent := &entry{
		key:              item.Key,
		value:            append([]byte(nil), item.Value...),
		version:          item.Version,
		namespaceVersion: item.NamespaceVersion,
		spaceVersion:     item.SpaceVersion,
		expireAt:         item.ExpireAt,
		createdAt:        item.CreatedAt,
		updatedAt:        item.UpdatedAt,
		lastAccessAt:     item.LastAccessAt,
		accessCount:      item.AccessCount,
	}
	ent.costBytes = costOf(ent.key, ent.value)
	if ent.version == 0 {
		ent.version = 1
	}
	return ent, nil
}
