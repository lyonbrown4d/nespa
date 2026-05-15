package cachewire

func EncodeBatchSetRequest(request BatchSetRequest) ([]byte, []byte, error) {
	packed, payload, err := PackBatchSet(request)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendCount(raw, setRequestCount(packed.Items))
	for index := range packed.Items {
		raw = appendBatchSetItem(raw, packed.Items[index])
	}
	return raw, payload, nil
}

func DecodeBatchSetRequest(raw, payload []byte) (BatchSetRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return BatchSetRequest{}, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return BatchSetRequest{}, err
	}
	items := make([]SetRequest, 0, count)
	for range count {
		item, readErr := cursor.readBatchSetItem()
		if readErr != nil {
			return BatchSetRequest{}, readErr
		}
		items = append(items, item)
	}
	if eofErr := cursor.ensureEOF(); eofErr != nil {
		return BatchSetRequest{}, eofErr
	}
	items, err = UnpackBatchSet(BatchSetRequest{Items: items}, payload)
	if err != nil {
		return BatchSetRequest{}, err
	}
	return BatchSetRequest{Items: items}, nil
}

func EncodeBatchGetRequest(request BatchGetRequest) []byte {
	raw := newMetadata()
	raw = appendCount(raw, getRequestCount(request.Items))
	for index := range request.Items {
		raw = appendBatchGetItem(raw, request.Items[index])
	}
	return raw
}

func DecodeBatchGetRequest(raw []byte) (BatchGetRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return BatchGetRequest{}, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return BatchGetRequest{}, err
	}
	items := make([]GetRequest, 0, count)
	for range count {
		item, readErr := cursor.readBatchGetItem()
		if readErr != nil {
			return BatchGetRequest{}, readErr
		}
		items = append(items, item)
	}
	if err := cursor.ensureEOF(); err != nil {
		return BatchGetRequest{}, err
	}
	return BatchGetRequest{Items: items}, nil
}

func EncodeBatchSetResponse(response BatchSetResponse) []byte {
	raw := newMetadata()
	raw = appendCount(raw, recordCount(response.Records))
	for index := range response.Records {
		raw = appendBatchRecord(raw, response.Records[index])
	}
	return raw
}

func DecodeBatchSetResponse(raw []byte) (BatchSetResponse, error) {
	records, err := decodeBatchRecords(raw)
	if err != nil {
		return BatchSetResponse{}, err
	}
	return BatchSetResponse{Records: records}, nil
}

func EncodeBatchGetResponse(response BatchGetResponse) ([]byte, []byte, error) {
	packed, payload, err := PackRecords(response.Records)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendCount(raw, recordCount(packed.Records))
	for index := range packed.Records {
		raw = appendBatchRecord(raw, packed.Records[index])
	}
	return raw, payload, nil
}

func DecodeBatchGetResponse(raw, payload []byte) (BatchGetResponse, error) {
	records, err := decodeBatchRecords(raw)
	if err != nil {
		return BatchGetResponse{}, err
	}
	records, err = UnpackRecords(BatchGetResponse{Records: records}, payload)
	if err != nil {
		return BatchGetResponse{}, err
	}
	return BatchGetResponse{Records: records}, nil
}

func appendCount(raw []byte, count uint64) []byte {
	return appendUint64(raw, count)
}

func setRequestCount(items []SetRequest) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func getRequestCount(items []GetRequest) uint64 {
	var count uint64
	for range items {
		count++
	}
	return count
}

func recordCount(records []Record) uint64 {
	var count uint64
	for range records {
		count++
	}
	return count
}

func appendBatchSetItem(raw []byte, item SetRequest) []byte {
	raw = appendKey(raw, item.Key)
	raw = appendInt64(raw, item.TTLMillis)
	raw = appendUint64(raw, item.NamespaceVersion)
	raw = appendUint64(raw, item.SpaceVersion)
	raw = appendUint64(raw, item.ExpectedVersion)
	raw = appendUint32(raw, item.PayloadOffset)
	return appendUint32(raw, item.PayloadSize)
}

func appendBatchGetItem(raw []byte, item GetRequest) []byte {
	raw = appendKey(raw, item.Key)
	raw = appendUint64(raw, item.NamespaceVersion)
	return appendUint64(raw, item.SpaceVersion)
}

func appendBatchRecord(raw []byte, record Record) []byte {
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
	raw = appendUint32(raw, record.PayloadOffset)
	return appendUint32(raw, record.PayloadSize)
}

func decodeBatchRecords(raw []byte) ([]Record, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return nil, err
	}
	count, err := cursor.readCount()
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, count)
	for range count {
		record, readErr := cursor.readBatchRecord()
		if readErr != nil {
			return nil, readErr
		}
		records = append(records, record)
	}
	if err := cursor.ensureEOF(); err != nil {
		return nil, err
	}
	return records, nil
}
