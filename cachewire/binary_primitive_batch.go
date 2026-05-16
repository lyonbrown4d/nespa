package cachewire

func EncodeBatchPrimitiveRequest(request BatchPrimitiveRequest) ([]byte, []byte, error) {
	packed, payload, err := packPrimitiveRequests(request.Items)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendCount(raw, primitiveRequestCount(packed))
	for index := range packed {
		raw = appendPrimitiveRequest(raw, packed[index])
	}
	return raw, payload, nil
}

func DecodeBatchPrimitiveRequest(raw, payload []byte) (BatchPrimitiveRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return BatchPrimitiveRequest{}, err
	}
	items, err := cursor.readPrimitiveRequests()
	if err != nil {
		return BatchPrimitiveRequest{}, err
	}
	if eofErr := cursor.ensureEOF(); eofErr != nil {
		return BatchPrimitiveRequest{}, eofErr
	}
	items, err = unpackPrimitiveRequests(items, payload)
	if err != nil {
		return BatchPrimitiveRequest{}, err
	}
	return BatchPrimitiveRequest{Items: items}, nil
}

func EncodeBatchPrimitiveResponse(response BatchPrimitiveResponse) ([]byte, []byte, error) {
	packed, payload, err := packPrimitiveResults(response.Results)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendCount(raw, primitiveResultCount(packed))
	for index := range packed {
		raw = appendPrimitiveResult(raw, packed[index])
	}
	return raw, payload, nil
}

func DecodeBatchPrimitiveResponse(raw, payload []byte) (BatchPrimitiveResponse, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return BatchPrimitiveResponse{}, err
	}
	results, err := cursor.readPrimitiveResults()
	if err != nil {
		return BatchPrimitiveResponse{}, err
	}
	if eofErr := cursor.ensureEOF(); eofErr != nil {
		return BatchPrimitiveResponse{}, eofErr
	}
	results, err = unpackPrimitiveResults(results, payload)
	if err != nil {
		return BatchPrimitiveResponse{}, err
	}
	return BatchPrimitiveResponse{Results: results}, nil
}

func (c *metadataCursor) readPrimitiveRequests() ([]PrimitiveRequest, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	items := make([]PrimitiveRequest, 0, count)
	for range count {
		item, readErr := c.readPrimitiveRequest()
		if readErr != nil {
			return nil, readErr
		}
		items = append(items, item)
	}
	return items, nil
}

func (c *metadataCursor) readPrimitiveResults() ([]PrimitiveResult, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	items := make([]PrimitiveResult, 0, count)
	for range count {
		item, readErr := c.readPrimitiveResult()
		if readErr != nil {
			return nil, readErr
		}
		items = append(items, item)
	}
	return items, nil
}
