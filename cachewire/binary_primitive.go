package cachewire

func EncodePrimitiveRequest(request PrimitiveRequest) ([]byte, []byte, error) {
	packed, payload, err := packPrimitiveRequest(request)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendPrimitiveRequest(raw, packed)
	return raw, payload, nil
}

func DecodePrimitiveRequest(raw, payload []byte) (PrimitiveRequest, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return PrimitiveRequest{}, err
	}
	request, err := cursor.readPrimitiveRequest()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return PrimitiveRequest{}, err
	}
	return unpackPrimitiveRequest(request, payload)
}

func EncodePrimitiveResponse(result PrimitiveResult) ([]byte, []byte, error) {
	packed, payload, err := packPrimitiveResult(result)
	if err != nil {
		return nil, nil, err
	}
	raw := newMetadata()
	raw = appendPrimitiveResult(raw, packed)
	return raw, payload, nil
}

func DecodePrimitiveResponse(raw, payload []byte) (PrimitiveResult, error) {
	cursor, err := newCursor(raw)
	if err != nil {
		return PrimitiveResult{}, err
	}
	result, err := cursor.readPrimitiveResult()
	if err != nil {
		return PrimitiveResult{}, err
	}
	if err := cursor.ensureEOF(); err != nil {
		return PrimitiveResult{}, err
	}
	return unpackPrimitiveResult(result, payload)
}
