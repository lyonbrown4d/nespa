package cachewire

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	binaryMetadataVersion byte = 1
	boolFalse             byte = 0
	boolTrue              byte = 1
	uint64Size                 = 8
)

func newMetadata() []byte {
	return []byte{binaryMetadataVersion}
}

func appendKey(raw []byte, key Key) []byte {
	raw = appendString(raw, key.Namespace)
	raw = appendString(raw, key.Space)
	raw = appendString(raw, key.Entity)
	return appendString(raw, key.Key)
}

func appendString(raw []byte, value string) []byte {
	raw = binary.AppendUvarint(raw, uint64(len(value)))
	return append(raw, value...)
}

func appendBool(raw []byte, value bool) []byte {
	if value {
		return append(raw, boolTrue)
	}
	return append(raw, boolFalse)
}

func appendUint64(raw []byte, value uint64) []byte {
	return binary.BigEndian.AppendUint64(raw, value)
}

func appendUint32(raw []byte, value uint32) []byte {
	return binary.BigEndian.AppendUint32(raw, value)
}

func appendInt64(raw []byte, value int64) []byte {
	return binary.AppendVarint(raw, value)
}

type metadataCursor struct {
	raw []byte
	pos int
}

func newCursor(raw []byte) (*metadataCursor, error) {
	if len(raw) == 0 {
		return nil, invalidMetadata("missing version")
	}
	if raw[0] != binaryMetadataVersion {
		return nil, invalidMetadata("unsupported version %d", raw[0])
	}
	return &metadataCursor{raw: raw, pos: 1}, nil
}

func (c *metadataCursor) readRecord() (Record, error) {
	namespace, err := c.readString()
	if err != nil {
		return Record{}, err
	}
	space, err := c.readString()
	if err != nil {
		return Record{}, err
	}
	entity, err := c.readString()
	if err != nil {
		return Record{}, err
	}
	key, err := c.readString()
	if err != nil {
		return Record{}, err
	}
	version, err := c.readUint64()
	if err != nil {
		return Record{}, err
	}
	namespaceVersion, err := c.readUint64()
	if err != nil {
		return Record{}, err
	}
	spaceVersion, err := c.readUint64()
	if err != nil {
		return Record{}, err
	}
	expireAtUnixMs, err := c.readInt64()
	if err != nil {
		return Record{}, err
	}
	if err := c.ensureEOF(); err != nil {
		return Record{}, err
	}
	return Record{
		Found:            true,
		Namespace:        namespace,
		Space:            space,
		Entity:           entity,
		Key:              key,
		Version:          version,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
		ExpireAtUnixMs:   expireAtUnixMs,
	}, nil
}

func (c *metadataCursor) readKey() (Key, error) {
	namespace, err := c.readString()
	if err != nil {
		return Key{}, err
	}
	space, err := c.readString()
	if err != nil {
		return Key{}, err
	}
	entity, err := c.readString()
	if err != nil {
		return Key{}, err
	}
	key, err := c.readString()
	if err != nil {
		return Key{}, err
	}
	return Key{Namespace: namespace, Space: space, Entity: entity, Key: key}, nil
}

func (c *metadataCursor) readString() (string, error) {
	size, count := binary.Uvarint(c.raw[c.pos:])
	if count <= 0 {
		return "", invalidMetadata("invalid string size")
	}
	c.pos += count
	length, err := checkedStringLength(size, len(c.raw)-c.pos)
	if err != nil {
		return "", err
	}
	end := c.pos + length
	value := string(c.raw[c.pos:end])
	c.pos = end
	return value, nil
}

func (c *metadataCursor) readBool() (bool, error) {
	if c.pos >= len(c.raw) {
		return false, invalidMetadata("missing bool")
	}
	value := c.raw[c.pos]
	c.pos++
	switch value {
	case boolFalse:
		return false, nil
	case boolTrue:
		return true, nil
	default:
		return false, invalidMetadata("invalid bool %d", value)
	}
}

func (c *metadataCursor) readUint64() (uint64, error) {
	end := c.pos + uint64Size
	if end < c.pos || end > len(c.raw) {
		return 0, invalidMetadata("missing uint64")
	}
	value := binary.BigEndian.Uint64(c.raw[c.pos:end])
	c.pos = end
	return value, nil
}

func (c *metadataCursor) readUint32() (uint32, error) {
	end := c.pos + 4
	if end < c.pos || end > len(c.raw) {
		return 0, invalidMetadata("missing uint32")
	}
	value := binary.BigEndian.Uint32(c.raw[c.pos:end])
	c.pos = end
	return value, nil
}

func (c *metadataCursor) readInt64() (int64, error) {
	value, count := binary.Varint(c.raw[c.pos:])
	if count <= 0 {
		return 0, invalidMetadata("invalid int64")
	}
	c.pos += count
	return value, nil
}

func (c *metadataCursor) readCount() (int, error) {
	count, err := c.readUint64()
	if err != nil {
		return 0, err
	}
	return checkedStringLength(count, len(c.raw)-c.pos)
}

func (c *metadataCursor) ensureEOF() error {
	if c.pos != len(c.raw) {
		return invalidMetadata("trailing bytes")
	}
	return nil
}

func checkedStringLength(size uint64, remaining int) (int, error) {
	if size > math.MaxInt {
		return 0, invalidMetadata("string exceeds metadata")
	}
	length := int(size)
	if length > remaining {
		return 0, invalidMetadata("string exceeds metadata")
	}
	return length, nil
}

func invalidMetadata(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMetadata, fmt.Sprintf(format, args...))
}
