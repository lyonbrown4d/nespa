package engine

import (
	"sort"

	collectionset "github.com/arcgolabs/collectionx/set"
)

func (s *shardWorker) applySetAdd(cmd shardCommand) shardResult {
	members, ent, exists, ok, err := s.mutableSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if exists && members.Contains(cmd.primitive.Member) {
		result := primitiveWriteResult(ent, checkedUint64(members.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	members.Add(cmd.primitive.Member)
	result := s.writeSetCollection(cmd, ent, exists, members)
	result.primitive.Bool = true
	return result
}

func (s *shardWorker) applySetRemove(cmd shardCommand) shardResult {
	members, ent, exists, ok, err := s.mutableSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if !exists {
		return primitiveMiss()
	}
	removed := members.Remove(cmd.primitive.Member)
	if !removed {
		result := primitiveWriteResult(ent, checkedUint64(members.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	result := s.writeSetCollection(cmd, ent, true, members)
	result.primitive.Bool = true
	return result
}

func (s *shardWorker) applySetContains(cmd shardCommand) shardResult {
	members, ent, ok, err := s.readSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Bool:   members.Contains(cmd.primitive.Member),
		Count:  checkedUint64(members.Len()),
	}}
}

func (s *shardWorker) applySetMembers(cmd shardCommand) shardResult {
	members, ent, ok, err := s.readSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record:  ent.record(),
		Found:   true,
		Count:   checkedUint64(members.Len()),
		Members: sortedMembers(members),
	}}
}

func (s *shardWorker) mutableSet(cmd shardCommand) (*collectionset.Set[string], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionset.NewSet[string](), nil, false, true, nil
	}
	members, err := decodeSetCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return members, ent, true, true, nil
}

func (s *shardWorker) readSet(cmd shardCommand) (*collectionset.Set[string], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	members, err := decodeSetCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return members, ent, true, nil
}

func (s *shardWorker) writeSetCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	members *collectionset.Set[string],
) shardResult {
	value, err := encodeSetCollection(members)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(members.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(members.Len()))}
}

func sortedMembers(members *collectionset.Set[string]) []string {
	out := members.Values()
	sort.Strings(out)
	return out
}
