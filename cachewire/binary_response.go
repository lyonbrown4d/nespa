package cachewire

func EncodeRecord(record Record) []byte {
	raw := newMetadata()
	raw = appendBool(raw, record.Found)
	if !record.Found {
		return raw
	}
	raw = appendString(raw, record.Namespace)
	raw = appendString(raw, record.Space)
	raw = appendString(raw, record.Entity)
	raw = appendString(raw, record.Key)
	raw = appendUint64(raw, record.Version)
	raw = appendUint64(raw, record.NamespaceVersion)
	raw = appendUint64(raw, record.SpaceVersion)
	raw = appendInt64(raw, record.ExpireAtUnixMs)
	return raw
}

func DecodeRecord(raw []byte) (Record, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return Record{}, err
	}
	found, err := cursor.readBool()
	if err != nil {
		return Record{}, err
	}
	if !found {
		if err := cursor.ensureEOF(); err != nil {
			return Record{}, err
		}
		return Record{Found: false}, nil
	}
	return cursor.readRecord()
}

func EncodeDeleteResponse(response DeleteResponse) []byte {
	return encodeBool(response.Deleted)
}

func DecodeDeleteResponse(raw []byte) (DeleteResponse, error) {
	value, err := decodeBool(raw)
	if err != nil {
		return DeleteResponse{}, err
	}
	return DeleteResponse{Deleted: value}, nil
}

func EncodeExistsResponse(response ExistsResponse) []byte {
	return encodeBool(response.Exists)
}

func DecodeExistsResponse(raw []byte) (ExistsResponse, error) {
	value, err := decodeBool(raw)
	if err != nil {
		return ExistsResponse{}, err
	}
	return ExistsResponse{Exists: value}, nil
}

func EncodeTouchResponse(response TouchResponse) []byte {
	return encodeBool(response.Touched)
}

func DecodeTouchResponse(raw []byte) (TouchResponse, error) {
	value, err := decodeBool(raw)
	if err != nil {
		return TouchResponse{}, err
	}
	return TouchResponse{Touched: value}, nil
}

func decodeVersionedKey(raw []byte) (Key, uint64, uint64, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return Key{}, 0, 0, err
	}
	key, err := cursor.readKey()
	if err != nil {
		return Key{}, 0, 0, err
	}
	namespaceVersion, err := cursor.readUint64()
	if err != nil {
		return Key{}, 0, 0, err
	}
	spaceVersion, err := cursor.readUint64()
	if err != nil {
		return Key{}, 0, 0, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return Key{}, 0, 0, err
	}
	return key, namespaceVersion, spaceVersion, nil
}

func encodeBool(value bool) []byte {
	raw := newMetadata()
	return appendBool(raw, value)
}

func decodeBool(raw []byte) (bool, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return false, err
	}
	value, err := cursor.readBool()
	if err != nil {
		return false, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return false, err
	}
	return value, nil
}
