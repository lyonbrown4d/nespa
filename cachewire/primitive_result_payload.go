package cachewire

func packPrimitiveResult(result PrimitiveResult) (PrimitiveResult, []byte, error) {
	payload := make([]byte, 0, primitiveResultPayloadSize(result))
	return appendPrimitiveResultPayload(result, payload)
}

func packPrimitiveResults(items []PrimitiveResult) ([]PrimitiveResult, []byte, error) {
	packed := make([]PrimitiveResult, 0, len(items))
	payload := make([]byte, 0, primitiveResultsPayloadSize(items))
	for index := range items {
		item, nextPayload, err := appendPrimitiveResultPayload(items[index], payload)
		if err != nil {
			return nil, nil, err
		}
		packed = append(packed, item)
		payload = nextPayload
	}
	return packed, payload, nil
}

func unpackPrimitiveResult(result PrimitiveResult, payload []byte) (PrimitiveResult, error) {
	value, err := SlicePayload(payload, result.PayloadOffset, result.PayloadSize)
	if err != nil {
		return PrimitiveResult{}, err
	}
	result.Value = value
	fields, err := unpackMapFields(result.Fields, payload)
	if err != nil {
		return PrimitiveResult{}, err
	}
	result.Fields = fields
	values, err := unpackListValues(result.Values, payload)
	if err != nil {
		return PrimitiveResult{}, err
	}
	result.Values = values
	return result, nil
}

func unpackPrimitiveResults(items []PrimitiveResult, payload []byte) ([]PrimitiveResult, error) {
	out := make([]PrimitiveResult, 0, len(items))
	for index := range items {
		item, err := unpackPrimitiveResult(items[index], payload)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func appendPrimitiveResultPayload(result PrimitiveResult, payload []byte) (PrimitiveResult, []byte, error) {
	var err error
	result, payload, err = appendPrimitiveValuePayload(result, payload)
	if err != nil {
		return PrimitiveResult{}, nil, err
	}
	result.Fields, payload, err = appendMapFieldsPayload(result.Fields, payload)
	if err != nil {
		return PrimitiveResult{}, nil, err
	}
	result.Values, payload, err = appendListValuesPayload(result.Values, payload)
	if err != nil {
		return PrimitiveResult{}, nil, err
	}
	return result, payload, nil
}

func appendPrimitiveValuePayload(result PrimitiveResult, payload []byte) (PrimitiveResult, []byte, error) {
	offset, size, err := checkedPayloadRange(len(payload), len(result.Value))
	if err != nil {
		return PrimitiveResult{}, nil, err
	}
	payload = append(payload, result.Value...)
	result.PayloadOffset = offset
	result.PayloadSize = size
	result.Value = nil
	return result, payload, nil
}

func appendMapFieldsPayload(fields []MapField, payload []byte) ([]MapField, []byte, error) {
	out := make([]MapField, 0, len(fields))
	for index := range fields {
		item, nextPayload, err := appendMapFieldPayload(fields[index], payload)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, item)
		payload = nextPayload
	}
	return out, payload, nil
}

func appendMapFieldPayload(field MapField, payload []byte) (MapField, []byte, error) {
	offset, size, err := checkedPayloadRange(len(payload), len(field.Value))
	if err != nil {
		return MapField{}, nil, err
	}
	payload = append(payload, field.Value...)
	field.PayloadOffset = offset
	field.PayloadSize = size
	field.Value = nil
	return field, payload, nil
}

func unpackMapFields(fields []MapField, payload []byte) ([]MapField, error) {
	out := make([]MapField, 0, len(fields))
	for index := range fields {
		value, err := SlicePayload(payload, fields[index].PayloadOffset, fields[index].PayloadSize)
		if err != nil {
			return nil, err
		}
		field := fields[index]
		field.Value = value
		out = append(out, field)
	}
	return out, nil
}

func appendListValuesPayload(values []ListValue, payload []byte) ([]ListValue, []byte, error) {
	out := make([]ListValue, 0, len(values))
	for index := range values {
		item, nextPayload, err := appendListValuePayload(values[index], payload)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, item)
		payload = nextPayload
	}
	return out, payload, nil
}

func appendListValuePayload(value ListValue, payload []byte) (ListValue, []byte, error) {
	offset, size, err := checkedPayloadRange(len(payload), len(value.Value))
	if err != nil {
		return ListValue{}, nil, err
	}
	payload = append(payload, value.Value...)
	value.PayloadOffset = offset
	value.PayloadSize = size
	value.Value = nil
	return value, payload, nil
}

func unpackListValues(values []ListValue, payload []byte) ([]ListValue, error) {
	out := make([]ListValue, 0, len(values))
	for index := range values {
		value, err := SlicePayload(payload, values[index].PayloadOffset, values[index].PayloadSize)
		if err != nil {
			return nil, err
		}
		item := values[index]
		item.Value = value
		out = append(out, item)
	}
	return out, nil
}
