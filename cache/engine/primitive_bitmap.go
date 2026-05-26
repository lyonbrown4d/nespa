package engine

import (
	"sort"

	collectionset "github.com/arcgolabs/collectionx/set"
)

func (s *shardWorker) applyBitmapSetBit(cmd shardCommand) shardResult {
	bits, ent, exists, ok, err := s.mutableBitmap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	offset := int(cmd.primitive.Delta)
	previous := bits.Contains(offset)
	next := cmd.primitive.InitialValue == 1
	if previous == next {
		if exists {
			result := primitiveWriteResult(ent, checkedUint64(bits.Len()))
			result.Bool = previous
			return shardResult{primitive: result}
		}
		return shardResult{primitive: PrimitiveResult{Applied: true, Bool: previous}}
	}
	if next {
		bits.Add(offset)
	} else {
		bits.Remove(offset)
	}
	result := s.writeBitmapCollection(cmd, ent, exists, bits)
	result.primitive.Bool = previous
	return result
}

func (s *shardWorker) applyBitmapGetBit(cmd shardCommand) shardResult {
	bits, ent, ok, err := s.readBitmap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Bool:   bits.Contains(int(cmd.primitive.Delta)),
		Count:  checkedUint64(bits.Len()),
	}}
}

func (s *shardWorker) applyBitmapBitCount(cmd shardCommand) shardResult {
	bits, ent, ok, err := s.readBitmap(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Count:  bitmapCount(bits, cmd.primitive.Start, cmd.primitive.Limit),
	}}
}

func (s *shardWorker) mutableBitmap(cmd shardCommand) (*collectionset.Set[int], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionset.NewSet[int](), nil, false, true, nil
	}
	bits, err := decodeBitmapCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return bits, ent, true, true, nil
}

func (s *shardWorker) readBitmap(cmd shardCommand) (*collectionset.Set[int], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	bits, err := decodeBitmapCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return bits, ent, true, nil
}

func (s *shardWorker) writeBitmapCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	bits *collectionset.Set[int],
) shardResult {
	value, err := encodeBitmapCollection(bits)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(bits.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(bits.Len()))}
}

func bitmapCount(bits *collectionset.Set[int], start int64, limit uint64) uint64 {
	if limit == 0 && start == 0 {
		return checkedUint64(bits.Len())
	}
	var count uint64
	startValue := checkedUint64FromInt64(start)
	end := startValue + limit
	if end < startValue {
		end = ^uint64(0)
	}
	values := bits.Values()
	sort.Ints(values)
	for _, bit := range values {
		value := checkedUint64(bit)
		if value < startValue {
			continue
		}
		if limit > 0 && value >= end {
			break
		}
		count++
	}
	return count
}

func checkedUint64FromInt64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
