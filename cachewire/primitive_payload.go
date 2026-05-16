package cachewire

func packPrimitiveRequest(request PrimitiveRequest) (PrimitiveRequest, []byte, error) {
	payload := append([]byte(nil), request.Value...)
	offset, size, err := checkedPayloadRange(0, len(payload))
	if err != nil {
		return PrimitiveRequest{}, nil, err
	}
	request.PayloadOffset = offset
	request.PayloadSize = size
	request.Value = nil
	return request, payload, nil
}

func packPrimitiveRequests(items []PrimitiveRequest) ([]PrimitiveRequest, []byte, error) {
	packed := make([]PrimitiveRequest, 0, len(items))
	payload := make([]byte, 0, primitiveRequestPayloadSize(items))
	for index := range items {
		item, nextPayload, err := appendPrimitiveRequestPayload(items[index], payload)
		if err != nil {
			return nil, nil, err
		}
		packed = append(packed, item)
		payload = nextPayload
	}
	return packed, payload, nil
}

func unpackPrimitiveRequest(request PrimitiveRequest, payload []byte) (PrimitiveRequest, error) {
	value, err := SlicePayload(payload, request.PayloadOffset, request.PayloadSize)
	if err != nil {
		return PrimitiveRequest{}, err
	}
	request.Value = value
	return request, nil
}

func unpackPrimitiveRequests(items []PrimitiveRequest, payload []byte) ([]PrimitiveRequest, error) {
	out := make([]PrimitiveRequest, 0, len(items))
	for index := range items {
		item, err := unpackPrimitiveRequest(items[index], payload)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func appendPrimitiveRequestPayload(
	request PrimitiveRequest,
	payload []byte,
) (PrimitiveRequest, []byte, error) {
	offset, size, err := checkedPayloadRange(len(payload), len(request.Value))
	if err != nil {
		return PrimitiveRequest{}, nil, err
	}
	payload = append(payload, request.Value...)
	request.PayloadOffset = offset
	request.PayloadSize = size
	request.Value = nil
	return request, payload, nil
}

func primitiveRequestPayloadSize(items []PrimitiveRequest) int {
	var total int
	for index := range items {
		total += len(items[index].Value)
	}
	return total
}
