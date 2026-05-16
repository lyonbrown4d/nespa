package cachewire

func appendPrimitiveResult(raw []byte, result PrimitiveResult) []byte {
	raw = appendBool(raw, result.Found)
	raw = appendBool(raw, result.Applied)
	raw = appendBatchRecord(raw, result.Record)
	raw = appendBool(raw, result.Bool)
	raw = appendUint64(raw, result.Count)
	raw = appendUint32(raw, result.PayloadOffset)
	raw = appendUint32(raw, result.PayloadSize)
	raw = appendMapFields(raw, result.Fields)
	raw = appendMembers(raw, result.Members)
	raw = appendScoredMembers(raw, result.ScoredMembers)
	return appendListValues(raw, result.Values)
}

func (c *metadataCursor) readPrimitiveResult() (PrimitiveResult, error) {
	found, applied, err := c.readResultState()
	if err != nil {
		return PrimitiveResult{}, err
	}
	record, err := c.readBatchRecord()
	if err != nil {
		return PrimitiveResult{}, err
	}
	return c.readPrimitiveResultBody(found, applied, record)
}

func (c *metadataCursor) readPrimitiveResultBody(
	found bool,
	applied bool,
	record Record,
) (PrimitiveResult, error) {
	boolValue, count, valueRange, err := c.readResultNumbers()
	if err != nil {
		return PrimitiveResult{}, err
	}
	fields, err := c.readMapFields()
	if err != nil {
		return PrimitiveResult{}, err
	}
	members, err := c.readMembers()
	if err != nil {
		return PrimitiveResult{}, err
	}
	scored, err := c.readScoredMembers()
	if err != nil {
		return PrimitiveResult{}, err
	}
	values, err := c.readListValues()
	if err != nil {
		return PrimitiveResult{}, err
	}
	return PrimitiveResult{
		Record:        record,
		Found:         found,
		Applied:       applied,
		Bool:          boolValue,
		Count:         count,
		Fields:        fields,
		Members:       members,
		ScoredMembers: scored,
		Values:        values,
		PayloadOffset: valueRange.offset,
		PayloadSize:   valueRange.size,
	}, nil
}

func (c *metadataCursor) readResultState() (bool, bool, error) {
	found, err := c.readBool()
	if err != nil {
		return false, false, err
	}
	applied, err := c.readBool()
	if err != nil {
		return false, false, err
	}
	return found, applied, nil
}

func (c *metadataCursor) readResultNumbers() (bool, uint64, payloadRange, error) {
	boolValue, err := c.readBool()
	if err != nil {
		return false, 0, payloadRange{}, err
	}
	count, err := c.readUint64()
	if err != nil {
		return false, 0, payloadRange{}, err
	}
	valueRange, err := c.readPayloadRange()
	if err != nil {
		return false, 0, payloadRange{}, err
	}
	return boolValue, count, valueRange, nil
}

func appendMapFields(raw []byte, fields []MapField) []byte {
	raw = appendCount(raw, mapFieldCount(fields))
	for index := range fields {
		raw = appendString(raw, fields[index].Field)
		raw = appendUint32(raw, fields[index].PayloadOffset)
		raw = appendUint32(raw, fields[index].PayloadSize)
	}
	return raw
}

func (c *metadataCursor) readMapFields() ([]MapField, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	fields := make([]MapField, 0, count)
	for range count {
		field, readErr := c.readMapField()
		if readErr != nil {
			return nil, readErr
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func (c *metadataCursor) readMapField() (MapField, error) {
	field, err := c.readString()
	if err != nil {
		return MapField{}, err
	}
	valueRange, err := c.readPayloadRange()
	if err != nil {
		return MapField{}, err
	}
	return MapField{Field: field, PayloadOffset: valueRange.offset, PayloadSize: valueRange.size}, nil
}

func appendListValues(raw []byte, values []ListValue) []byte {
	raw = appendCount(raw, listValueCount(values))
	for index := range values {
		raw = appendUint32(raw, values[index].PayloadOffset)
		raw = appendUint32(raw, values[index].PayloadSize)
	}
	return raw
}

func (c *metadataCursor) readListValues() ([]ListValue, error) {
	count, err := c.readCount()
	if err != nil {
		return nil, err
	}
	values := make([]ListValue, 0, count)
	for range count {
		valueRange, readErr := c.readPayloadRange()
		if readErr != nil {
			return nil, readErr
		}
		values = append(values, ListValue{
			PayloadOffset: valueRange.offset,
			PayloadSize:   valueRange.size,
		})
	}
	return values, nil
}
