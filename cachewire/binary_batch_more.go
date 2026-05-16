package cachewire

func EncodeBatchDeleteRequest(request BatchDeleteRequest) []byte {
	raw := newMetadata()
	raw = appendCount(raw, deleteRequestCount(request.Items))
	for index := range request.Items {
		raw = appendBatchDeleteItem(raw, request.Items[index])
	}
	return raw
}

func DecodeBatchDeleteRequest(raw []byte) (BatchDeleteRequest, error) {
	items, err := decodeBatchItems(raw, (*metadataCursor).readBatchDeleteItem)
	if err != nil {
		return BatchDeleteRequest{}, err
	}
	return BatchDeleteRequest{Items: items}, nil
}

func EncodeBatchExistsRequest(request BatchExistsRequest) []byte {
	raw := newMetadata()
	raw = appendCount(raw, existsRequestCount(request.Items))
	for index := range request.Items {
		raw = appendBatchExistsItem(raw, request.Items[index])
	}
	return raw
}

func DecodeBatchExistsRequest(raw []byte) (BatchExistsRequest, error) {
	items, err := decodeBatchItems(raw, (*metadataCursor).readBatchExistsItem)
	if err != nil {
		return BatchExistsRequest{}, err
	}
	return BatchExistsRequest{Items: items}, nil
}

func EncodeBatchTouchRequest(request BatchTouchRequest) []byte {
	raw := newMetadata()
	raw = appendCount(raw, touchRequestCount(request.Items))
	for index := range request.Items {
		raw = appendBatchTouchItem(raw, request.Items[index])
	}
	return raw
}

func DecodeBatchTouchRequest(raw []byte) (BatchTouchRequest, error) {
	items, err := decodeBatchItems(raw, (*metadataCursor).readBatchTouchItem)
	if err != nil {
		return BatchTouchRequest{}, err
	}
	return BatchTouchRequest{Items: items}, nil
}

func EncodeBatchDeleteResponse(response BatchDeleteResponse) []byte {
	return encodeBatchBoolResponses(response.Results, func(item DeleteResponse) bool {
		return item.Deleted
	})
}

func DecodeBatchDeleteResponse(raw []byte) (BatchDeleteResponse, error) {
	values, err := decodeBatchBoolResponses(raw)
	if err != nil {
		return BatchDeleteResponse{}, err
	}
	results := make([]DeleteResponse, 0, len(values))
	for index := range values {
		results = append(results, DeleteResponse{Deleted: values[index]})
	}
	return BatchDeleteResponse{Results: results}, nil
}

func EncodeBatchExistsResponse(response BatchExistsResponse) []byte {
	return encodeBatchBoolResponses(response.Results, func(item ExistsResponse) bool {
		return item.Exists
	})
}

func DecodeBatchExistsResponse(raw []byte) (BatchExistsResponse, error) {
	values, err := decodeBatchBoolResponses(raw)
	if err != nil {
		return BatchExistsResponse{}, err
	}
	results := make([]ExistsResponse, 0, len(values))
	for index := range values {
		results = append(results, ExistsResponse{Exists: values[index]})
	}
	return BatchExistsResponse{Results: results}, nil
}

func EncodeBatchTouchResponse(response BatchTouchResponse) []byte {
	return encodeBatchBoolResponses(response.Results, func(item TouchResponse) bool {
		return item.Touched
	})
}

func DecodeBatchTouchResponse(raw []byte) (BatchTouchResponse, error) {
	values, err := decodeBatchBoolResponses(raw)
	if err != nil {
		return BatchTouchResponse{}, err
	}
	results := make([]TouchResponse, 0, len(values))
	for index := range values {
		results = append(results, TouchResponse{Touched: values[index]})
	}
	return BatchTouchResponse{Results: results}, nil
}
