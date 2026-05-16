package cachewire

func decodeBatchItems[T any](
	raw []byte,
	read func(*metadataCursor) (T, error),
) ([]T, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return nil, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return nil, err
	}
	items := make([]T, 0, count)
	for range count {
		item, readErr := read(cursor)
		if readErr != nil {
			return nil, readErr
		}
		items = append(items, item)
	}
	if err := cursor.ensureEOF(); err != nil {
		return nil, err
	}
	return items, nil
}

func encodeBatchBoolResponses[T any](items []T, value func(T) bool) []byte {
	raw := newMetadata()
	raw = appendCount(raw, itemCount(items))
	for index := range items {
		raw = appendBool(raw, value(items[index]))
	}
	return raw
}

func decodeBatchBoolResponses(raw []byte) ([]bool, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return nil, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return nil, err
	}
	values := make([]bool, 0, count)
	for range count {
		value, readErr := cursor.readBool()
		if readErr != nil {
			return nil, readErr
		}
		values = append(values, value)
	}
	if err := cursor.ensureEOF(); err != nil {
		return nil, err
	}
	return values, nil
}

func itemCount[T any](items []T) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func deleteRequestCount(items []DeleteRequest) uint64 {
	return itemCount(items)
}

func existsRequestCount(items []ExistsRequest) uint64 {
	return itemCount(items)
}

func touchRequestCount(items []TouchRequest) uint64 {
	return itemCount(items)
}

func appendBatchDeleteItem(raw []byte, item DeleteRequest) []byte {
	raw = appendKey(raw, item.Key)
	return appendUint64(raw, item.ExpectedVersion)
}

func appendBatchExistsItem(raw []byte, item ExistsRequest) []byte {
	raw = appendKey(raw, item.Key)
	raw = appendUint64(raw, item.NamespaceVersion)
	return appendUint64(raw, item.SpaceVersion)
}

func appendBatchTouchItem(raw []byte, item TouchRequest) []byte {
	raw = appendKey(raw, item.Key)
	raw = appendInt64(raw, item.TTLMillis)
	raw = appendUint64(raw, item.NamespaceVersion)
	raw = appendUint64(raw, item.SpaceVersion)
	return appendUint64(raw, item.ExpectedVersion)
}
