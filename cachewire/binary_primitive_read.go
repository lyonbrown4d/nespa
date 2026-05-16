package cachewire

import (
	"encoding/binary"
	"math"
)

func (c *metadataCursor) readPrimitiveKind() (PrimitiveKind, error) {
	value, err := c.readByte()
	if err != nil {
		return 0, err
	}
	return PrimitiveKind(value), nil
}

func (c *metadataCursor) readPrimitiveNames() (string, string, error) {
	field, err := c.readString()
	if err != nil {
		return "", "", err
	}
	member, err := c.readString()
	if err != nil {
		return "", "", err
	}
	return field, member, nil
}

func (c *metadataCursor) readPrimitiveNumbers() (primitiveNumbers, error) {
	delta, initialValue, err := c.readPrimitiveIntPair()
	if err != nil {
		return primitiveNumbers{}, err
	}
	score, minScore, maxScore, err := c.readPrimitiveScores()
	if err != nil {
		return primitiveNumbers{}, err
	}
	limit, err := c.readUint64()
	if err != nil {
		return primitiveNumbers{}, err
	}
	start, err := c.readInt64()
	if err != nil {
		return primitiveNumbers{}, err
	}
	return primitiveNumbers{
		delta:        delta,
		initialValue: initialValue,
		score:        score,
		minScore:     minScore,
		maxScore:     maxScore,
		limit:        limit,
		start:        start,
	}, nil
}

func (c *metadataCursor) readPrimitiveIntPair() (int64, int64, error) {
	delta, err := c.readInt64()
	if err != nil {
		return 0, 0, err
	}
	initialValue, err := c.readInt64()
	if err != nil {
		return 0, 0, err
	}
	return delta, initialValue, nil
}

func (c *metadataCursor) readPrimitiveScores() (float64, float64, float64, error) {
	score, err := c.readFloat64()
	if err != nil {
		return 0, 0, 0, err
	}
	minScore, err := c.readFloat64()
	if err != nil {
		return 0, 0, 0, err
	}
	maxScore, err := c.readFloat64()
	if err != nil {
		return 0, 0, 0, err
	}
	return score, minScore, maxScore, nil
}

func (c *metadataCursor) readPrimitiveFlags() (bool, bool, bool, error) {
	hasMin, err := c.readBool()
	if err != nil {
		return false, false, false, err
	}
	hasMax, err := c.readBool()
	if err != nil {
		return false, false, false, err
	}
	reverse, err := c.readBool()
	if err != nil {
		return false, false, false, err
	}
	return hasMin, hasMax, reverse, nil
}

func (c *metadataCursor) readPayloadRange() (payloadRange, error) {
	offset, err := c.readUint32()
	if err != nil {
		return payloadRange{}, err
	}
	size, err := c.readUint32()
	if err != nil {
		return payloadRange{}, err
	}
	return payloadRange{offset: offset, size: size}, nil
}

func (c *metadataCursor) readByte() (byte, error) {
	if c.pos >= len(c.raw) {
		return 0, invalidMetadata("missing byte")
	}
	value := c.raw[c.pos]
	c.pos++
	return value, nil
}

func (c *metadataCursor) readFloat64() (float64, error) {
	end := c.pos + uint64Size
	if end < c.pos || end > len(c.raw) {
		return 0, invalidMetadata("missing float64")
	}
	value := math.Float64frombits(binary.BigEndian.Uint64(c.raw[c.pos:end]))
	c.pos = end
	return value, nil
}
