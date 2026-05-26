package engine

import (
	"bytes"
	"encoding/hex"
	"hash/fnv"
	"sort"

	collectionset "github.com/arcgolabs/collectionx/set"
)

func (s *shardWorker) applyHLLAdd(cmd shardCommand) shardResult {
	hashes, ent, exists, ok, err := s.mutableHLL(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	hash := hllHash(cmd.primitive.Member)
	if exists && hashes.Contains(hash) {
		result := primitiveWriteResult(ent, checkedUint64(hashes.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	hashes.Add(hash)
	result := s.writeHLLCollection(cmd, ent, exists, hashes)
	result.primitive.Bool = true
	return result
}

func (s *shardWorker) applyHLLCount(cmd shardCommand) shardResult {
	hashes, ent, ok, err := s.readHLL(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Count:  checkedUint64(hashes.Len()),
	}}
}

func (s *shardWorker) applyHLLMerge(cmd shardCommand) shardResult {
	hashes, ent, exists, ok, err := s.mutableHLL(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	for _, hash := range decodeHLLHashes(cmd.primitive.Value) {
		hashes.Add(hash)
	}
	return s.writeHLLCollection(cmd, ent, exists, hashes)
}

func (s *shardWorker) applyHLLMembers(cmd shardCommand) shardResult {
	hashes, ent, ok, err := s.readHLL(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	members := hashes.Values()
	sort.Strings(members)
	return shardResult{primitive: PrimitiveResult{
		Record:  ent.record(),
		Found:   true,
		Count:   checkedUint64(hashes.Len()),
		Members: members,
	}}
}

func (s *shardWorker) mutableHLL(cmd shardCommand) (*collectionset.Set[string], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionset.NewSet[string](), nil, false, true, nil
	}
	hashes, err := decodeHLLCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return hashes, ent, true, true, nil
}

func (s *shardWorker) readHLL(cmd shardCommand) (*collectionset.Set[string], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	hashes, err := decodeHLLCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return hashes, ent, true, nil
}

func (s *shardWorker) writeHLLCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	hashes *collectionset.Set[string],
) shardResult {
	value, err := encodeHLLCollection(hashes)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(hashes.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(hashes.Len()))}
}

func hllHash(member string) string {
	hash := fnv.New64a()
	if _, err := hash.Write([]byte(member)); err != nil {
		return ""
	}
	var raw [8]byte
	value := hash.Sum64()
	for index := 7; index >= 0; index-- {
		raw[index] = byte(value)
		value >>= 8
	}
	return hex.EncodeToString(raw[:])
}

func decodeHLLHashes(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	parts := bytes.Split(raw, []byte{0})
	hashes := make([]string, 0, len(parts))
	for index := range parts {
		if len(parts[index]) == 0 {
			continue
		}
		hashes = append(hashes, string(parts[index]))
	}
	return hashes
}
