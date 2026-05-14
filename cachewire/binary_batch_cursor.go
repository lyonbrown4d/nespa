package cachewire

func (c *metadataCursor) readBatchSetItem() (SetRequest, error) {
	key, err := c.readKey()
	if err != nil {
		return SetRequest{}, err
	}
	ttlMillis, err := c.readInt64()
	if err != nil {
		return SetRequest{}, err
	}
	namespaceVersion, err := c.readUint64()
	if err != nil {
		return SetRequest{}, err
	}
	spaceVersion, err := c.readUint64()
	if err != nil {
		return SetRequest{}, err
	}
	payloadOffset, err := c.readUint32()
	if err != nil {
		return SetRequest{}, err
	}
	payloadSize, err := c.readUint32()
	if err != nil {
		return SetRequest{}, err
	}
	return SetRequest{
		Key:              key,
		TTLMillis:        ttlMillis,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
		PayloadOffset:    payloadOffset,
		PayloadSize:      payloadSize,
	}, nil
}

func (c *metadataCursor) readBatchGetItem() (GetRequest, error) {
	key, err := c.readKey()
	if err != nil {
		return GetRequest{}, err
	}
	namespaceVersion, err := c.readUint64()
	if err != nil {
		return GetRequest{}, err
	}
	spaceVersion, err := c.readUint64()
	if err != nil {
		return GetRequest{}, err
	}
	return GetRequest{
		Key:              key,
		NamespaceVersion: namespaceVersion,
		SpaceVersion:     spaceVersion,
	}, nil
}

func (c *metadataCursor) readBatchRecord() (Record, error) {
	found, err := c.readBool()
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{Found: false}, nil
	}
	record, err := c.readBatchRecordFields()
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

func (c *metadataCursor) readBatchRecordFields() (Record, error) {
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
	return c.readBatchRecordNumbers(namespace, space, entity, key)
}

func (c *metadataCursor) readBatchRecordNumbers(namespace, space, entity, key string) (Record, error) {
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
	payloadOffset, err := c.readUint32()
	if err != nil {
		return Record{}, err
	}
	payloadSize, err := c.readUint32()
	if err != nil {
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
		PayloadOffset:    payloadOffset,
		PayloadSize:      payloadSize,
	}, nil
}
