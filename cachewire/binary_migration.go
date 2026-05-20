package cachewire

func EncodeMigrationRangeRequest(request MigrationRangeRequest) []byte {
	raw := newMetadata()
	raw = appendString(raw, request.Namespace)
	raw = appendString(raw, request.Space)
	raw = appendUint32(raw, request.VSlotStart)
	return appendUint32(raw, request.VSlotEnd)
}

func DecodeMigrationRangeRequest(raw []byte) (MigrationRangeRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	request, err := cursor.readMigrationRangeRequest()
	if err != nil {
		return MigrationRangeRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return MigrationRangeRequest{}, err
	}
	return request, nil
}

func EncodeMigrationSnapshot(snapshot MigrationSnapshot) ([]byte, []byte, error) {
	packed, payload, err := PackMigrationSnapshot(snapshot)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendCount(raw, migrationSnapshotEntryCount(packed.Entries))
	for index := range packed.Entries {
		raw = appendMigrationSnapshotEntry(raw, packed.Entries[index])
	}
	return raw, payload, nil
}

func DecodeMigrationSnapshot(raw, payload []byte) (MigrationSnapshot, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return MigrationSnapshot{}, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return MigrationSnapshot{}, err
	}
	entries, err := cursor.readMigrationSnapshotEntries(count)
	if err != nil {
		return MigrationSnapshot{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return MigrationSnapshot{}, err
	}
	return UnpackMigrationSnapshot(MigrationSnapshot{Entries: entries}, payload)
}

func EncodeMigrationImportResponse(response MigrationImportResponse) []byte {
	raw := newMetadata()
	return appendUint64(raw, response.Imported)
}

func DecodeMigrationImportResponse(raw []byte) (MigrationImportResponse, error) {
	value, err := decodeUint64Response(raw)
	if err != nil {
		return MigrationImportResponse{}, err
	}
	return MigrationImportResponse{Imported: value}, nil
}

func EncodeMigrationDeleteRangeResponse(response MigrationDeleteRangeResponse) []byte {
	raw := newMetadata()
	return appendUint64(raw, response.Deleted)
}

func DecodeMigrationDeleteRangeResponse(raw []byte) (MigrationDeleteRangeResponse, error) {
	value, err := decodeUint64Response(raw)
	if err != nil {
		return MigrationDeleteRangeResponse{}, err
	}
	return MigrationDeleteRangeResponse{Deleted: value}, nil
}

func EncodeMigrationFenceResponse(response MigrationFenceResponse) []byte {
	return encodeBool(response.Applied)
}

func DecodeMigrationFenceResponse(raw []byte) (MigrationFenceResponse, error) {
	value, err := decodeBool(raw)
	if err != nil {
		return MigrationFenceResponse{}, err
	}
	return MigrationFenceResponse{Applied: value}, nil
}

func decodeUint64Response(raw []byte) (uint64, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return 0, err
	}
	value, err := cursor.readUint64()
	if err != nil {
		return 0, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return 0, err
	}
	return value, nil
}
