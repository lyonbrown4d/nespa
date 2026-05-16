package cachewire

import (
	"encoding/binary"
	"math"
)

type primitiveOptions struct {
	ttlMillis        int64
	namespaceVersion uint64
	spaceVersion     uint64
	expectedVersion  uint64
}

type primitiveNumbers struct {
	delta        int64
	initialValue int64
	score        float64
	minScore     float64
	maxScore     float64
	limit        uint64
	start        int64
}

type payloadRange struct {
	offset uint32
	size   uint32
}

func appendPrimitiveRequest(raw []byte, item PrimitiveRequest) []byte {
	raw = append(raw, byte(item.Kind))
	raw = appendKey(raw, item.Key)
	raw = appendPrimitiveOptions(raw, item)
	raw = appendString(raw, item.Field)
	raw = appendString(raw, item.Member)
	raw = appendPrimitiveNumbers(raw, item)
	raw = appendBool(raw, item.HasMinScore)
	raw = appendBool(raw, item.HasMaxScore)
	raw = appendBool(raw, item.Reverse)
	raw = appendUint32(raw, item.PayloadOffset)
	return appendUint32(raw, item.PayloadSize)
}

func (c *metadataCursor) readPrimitiveRequest() (PrimitiveRequest, error) {
	kind, err := c.readPrimitiveKind()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	key, err := c.readKey()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	return c.readPrimitiveRequestBody(kind, key)
}

func (c *metadataCursor) readPrimitiveRequestBody(kind PrimitiveKind, key Key) (PrimitiveRequest, error) {
	opts, err := c.readPrimitiveOptions()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	field, member, err := c.readPrimitiveNames()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	return c.readPrimitiveRequestTail(kind, key, opts, field, member)
}

func (c *metadataCursor) readPrimitiveRequestTail(
	kind PrimitiveKind,
	key Key,
	opts primitiveOptions,
	field string,
	member string,
) (PrimitiveRequest, error) {
	numbers, err := c.readPrimitiveNumbers()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	hasMin, hasMax, reverse, err := c.readPrimitiveFlags()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	valueRange, err := c.readPayloadRange()
	if err != nil {
		return PrimitiveRequest{}, err
	}
	return buildPrimitiveRequest(kind, key, opts, field, member, numbers, hasMin, hasMax, reverse, valueRange), nil
}

func buildPrimitiveRequest(
	kind PrimitiveKind,
	key Key,
	opts primitiveOptions,
	field string,
	member string,
	numbers primitiveNumbers,
	hasMin bool,
	hasMax bool,
	reverse bool,
	valueRange payloadRange,
) PrimitiveRequest {
	return PrimitiveRequest{
		Key:              key,
		Kind:             kind,
		TTLMillis:        opts.ttlMillis,
		NamespaceVersion: opts.namespaceVersion,
		SpaceVersion:     opts.spaceVersion,
		ExpectedVersion:  opts.expectedVersion,
		Field:            field,
		Member:           member,
		Delta:            numbers.delta,
		InitialValue:     numbers.initialValue,
		Score:            numbers.score,
		MinScore:         numbers.minScore,
		MaxScore:         numbers.maxScore,
		HasMinScore:      hasMin,
		HasMaxScore:      hasMax,
		Limit:            numbers.limit,
		Start:            numbers.start,
		Reverse:          reverse,
		PayloadOffset:    valueRange.offset,
		PayloadSize:      valueRange.size,
	}
}

func appendPrimitiveOptions(raw []byte, item PrimitiveRequest) []byte {
	raw = appendInt64(raw, item.TTLMillis)
	raw = appendUint64(raw, item.NamespaceVersion)
	raw = appendUint64(raw, item.SpaceVersion)
	return appendUint64(raw, item.ExpectedVersion)
}

func appendPrimitiveNumbers(raw []byte, item PrimitiveRequest) []byte {
	raw = appendInt64(raw, item.Delta)
	raw = appendInt64(raw, item.InitialValue)
	raw = appendFloat64(raw, item.Score)
	raw = appendFloat64(raw, item.MinScore)
	raw = appendFloat64(raw, item.MaxScore)
	raw = appendUint64(raw, item.Limit)
	return appendInt64(raw, item.Start)
}

func (c *metadataCursor) readPrimitiveOptions() (primitiveOptions, error) {
	ttlMillis, err := c.readInt64()
	if err != nil {
		return primitiveOptions{}, err
	}
	namespaceVersion, spaceVersion, expectedVersion, err := c.readVersionTriplet()
	if err != nil {
		return primitiveOptions{}, err
	}
	return primitiveOptions{
		ttlMillis:        ttlMillis,
		namespaceVersion: namespaceVersion,
		spaceVersion:     spaceVersion,
		expectedVersion:  expectedVersion,
	}, nil
}

func (c *metadataCursor) readVersionTriplet() (uint64, uint64, uint64, error) {
	namespaceVersion, err := c.readUint64()
	if err != nil {
		return 0, 0, 0, err
	}
	spaceVersion, err := c.readUint64()
	if err != nil {
		return 0, 0, 0, err
	}
	expectedVersion, err := c.readUint64()
	if err != nil {
		return 0, 0, 0, err
	}
	return namespaceVersion, spaceVersion, expectedVersion, nil
}

func appendFloat64(raw []byte, value float64) []byte {
	return binary.BigEndian.AppendUint64(raw, math.Float64bits(value))
}
