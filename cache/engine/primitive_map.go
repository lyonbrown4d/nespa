package engine

import (
	"bytes"
	"sort"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *shardWorker) applyMapSet(cmd shardCommand) shardResult {
	fields, ent, exists, ok, err := s.mutableMap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if sameMapField(fields, cmd.primitive.Field, cmd.primitive.Value) && exists {
		return shardResult{primitive: primitiveWriteResult(ent, checkedUint64(fields.Len()))}
	}
	fields.Set(cmd.primitive.Field, append([]byte(nil), cmd.primitive.Value...))
	return s.writeMapCollection(cmd, ent, exists, fields)
}

func (s *shardWorker) applyMapGet(cmd shardCommand) shardResult {
	fields, ent, ok, err := s.readMap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	value, found := fields.Get(cmd.primitive.Field)
	result := PrimitiveResult{Record: ent.record(), Found: found, Count: checkedUint64(fields.Len())}
	if found {
		result.Value = append([]byte(nil), value...)
	}
	return shardResult{primitive: result}
}

func (s *shardWorker) applyMapDelete(cmd shardCommand) shardResult {
	fields, ent, exists, ok, err := s.mutableMap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if !exists {
		return primitiveMiss()
	}
	removed := fields.Delete(cmd.primitive.Field)
	if !removed {
		result := primitiveWriteResult(ent, checkedUint64(fields.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	result := s.writeMapCollection(cmd, ent, true, fields)
	result.primitive.Bool = true
	return result
}

func (s *shardWorker) applyMapGetAll(cmd shardCommand) shardResult {
	fields, ent, ok, err := s.readMap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Count:  checkedUint64(fields.Len()),
		Fields: mapFields(fields),
	}}
}

func (s *shardWorker) mutableMap(
	cmd shardCommand,
) (*collectionmapping.Map[string, []byte], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionmapping.NewMap[string, []byte](), nil, false, true, nil
	}
	fields, err := decodeMapCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return fields, ent, true, true, nil
}

func (s *shardWorker) readMap(cmd shardCommand) (*collectionmapping.Map[string, []byte], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	fields, err := decodeMapCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return fields, ent, true, nil
}

func (s *shardWorker) writeMapCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	fields *collectionmapping.Map[string, []byte],
) shardResult {
	value, err := encodeMapCollection(fields)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(fields.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(fields.Len()))}
}

func sameMapField(fields *collectionmapping.Map[string, []byte], field string, value []byte) bool {
	current, ok := fields.Get(field)
	return ok && bytes.Equal(current, value)
}

func mapFields(fields *collectionmapping.Map[string, []byte]) []MapField {
	out := make([]MapField, 0, fields.Len())
	fields.Range(func(field string, value []byte) bool {
		out = append(out, MapField{Field: field, Value: append([]byte(nil), value...)})
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		return out[i].Field < out[j].Field
	})
	return out
}
