package cachewire

import "errors"

var ErrInvalidMetadata = errors.New("cachewire: invalid metadata")

func EncodeSetRequest(request SetRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendInt64(raw, request.TTLMillis)
	raw = appendUint64(raw, request.NamespaceVersion)
	raw = appendUint64(raw, request.SpaceVersion)
	raw = appendUint64(raw, request.ExpectedVersion)
	return raw
}

func DecodeSetRequest(raw []byte) (SetRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return SetRequest{}, err
	}
	key, err := cursor.readKey()
	if err != nil {
		return SetRequest{}, err
	}
	ttlMillis, err := cursor.readInt64()
	if err != nil {
		return SetRequest{}, err
	}
	namespaceVersion, err := cursor.readUint64()
	if err != nil {
		return SetRequest{}, err
	}
	spaceVersion, err := cursor.readUint64()
	if err != nil {
		return SetRequest{}, err
	}
	expectedVersion, err := cursor.readUint64()
	if err != nil {
		return SetRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return SetRequest{}, err
	}
	return SetRequest{
		Key:              key,
		TTLMillis:        ttlMillis,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
		ExpectedVersion:  expectedVersion,
	}, nil
}

func EncodeGetRequest(request GetRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendUint64(raw, request.NamespaceVersion)
	raw = appendUint64(raw, request.SpaceVersion)
	return raw
}

func DecodeGetRequest(raw []byte) (GetRequest, error) {
	key, namespaceVersion, spaceVersion, err := decodeVersionedKey(raw)
	if err != nil {
		return GetRequest{}, err
	}
	return GetRequest{Key: key, NamespaceVersion: namespaceVersion, SpaceVersion: spaceVersion}, nil
}

func EncodeDeleteRequest(request DeleteRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendUint64(raw, request.ExpectedVersion)
	return raw
}

func DecodeDeleteRequest(raw []byte) (DeleteRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return DeleteRequest{}, err
	}
	key, err := cursor.readKey()
	if err != nil {
		return DeleteRequest{}, err
	}
	expectedVersion, err := cursor.readUint64()
	if err != nil {
		return DeleteRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return DeleteRequest{}, err
	}
	return DeleteRequest{Key: key, ExpectedVersion: expectedVersion}, nil
}

func EncodeExistsRequest(request ExistsRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendUint64(raw, request.NamespaceVersion)
	raw = appendUint64(raw, request.SpaceVersion)
	return raw
}

func DecodeExistsRequest(raw []byte) (ExistsRequest, error) {
	key, namespaceVersion, spaceVersion, err := decodeVersionedKey(raw)
	if err != nil {
		return ExistsRequest{}, err
	}
	return ExistsRequest{Key: key, NamespaceVersion: namespaceVersion, SpaceVersion: spaceVersion}, nil
}

func EncodeTouchRequest(request TouchRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendInt64(raw, request.TTLMillis)
	raw = appendUint64(raw, request.NamespaceVersion)
	raw = appendUint64(raw, request.SpaceVersion)
	raw = appendUint64(raw, request.ExpectedVersion)
	return raw
}

func DecodeTouchRequest(raw []byte) (TouchRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return TouchRequest{}, err
	}
	key, err := cursor.readKey()
	if err != nil {
		return TouchRequest{}, err
	}
	ttlMillis, err := cursor.readInt64()
	if err != nil {
		return TouchRequest{}, err
	}
	namespaceVersion, err := cursor.readUint64()
	if err != nil {
		return TouchRequest{}, err
	}
	spaceVersion, err := cursor.readUint64()
	if err != nil {
		return TouchRequest{}, err
	}
	expectedVersion, err := cursor.readUint64()
	if err != nil {
		return TouchRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return TouchRequest{}, err
	}
	return TouchRequest{
		Key:              key,
		TTLMillis:        ttlMillis,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
		ExpectedVersion:  expectedVersion,
	}, nil
}

func EncodeAdjustRequest(request AdjustRequest) []byte {
	raw := newMetadata()
	raw = appendKey(raw, request.Key)
	raw = appendInt64(raw, request.TTLMillis)
	raw = appendInt64(raw, request.InitialValue)
	raw = appendInt64(raw, request.Delta)
	raw = appendUint64(raw, request.NamespaceVersion)
	raw = appendUint64(raw, request.SpaceVersion)
	raw = appendUint64(raw, request.ExpectedVersion)
	return raw
}

func DecodeAdjustRequest(raw []byte) (AdjustRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return AdjustRequest{}, err
	}
	key, err := cursor.readKey()
	if err != nil {
		return AdjustRequest{}, err
	}
	ttlMillis, err := cursor.readInt64()
	if err != nil {
		return AdjustRequest{}, err
	}
	initialValue, err := cursor.readInt64()
	if err != nil {
		return AdjustRequest{}, err
	}
	delta, err := cursor.readInt64()
	if err != nil {
		return AdjustRequest{}, err
	}
	namespaceVersion, err := cursor.readUint64()
	if err != nil {
		return AdjustRequest{}, err
	}
	spaceVersion, err := cursor.readUint64()
	if err != nil {
		return AdjustRequest{}, err
	}
	expectedVersion, err := cursor.readUint64()
	if err != nil {
		return AdjustRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return AdjustRequest{}, err
	}
	return AdjustRequest{
		Key:              key,
		TTLMillis:        ttlMillis,
		InitialValue:     initialValue,
		Delta:            delta,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
		ExpectedVersion:  expectedVersion,
	}, nil
}

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
