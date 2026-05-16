package engine

import (
	"slices"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *shardWorker) applyListPushFront(cmd shardCommand) shardResult {
	return s.applyListPush(cmd, true)
}

func (s *shardWorker) applyListPushBack(cmd shardCommand) shardResult {
	return s.applyListPush(cmd, false)
}

func (s *shardWorker) applyListPush(cmd shardCommand, front bool) shardResult {
	values, ent, exists, ok, err := s.mutableList(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	value := append([]byte(nil), cmd.primitive.Value...)
	if front {
		values.AddAt(0, value)
	} else {
		values.Add(value)
	}
	return s.writeListCollection(cmd, ent, exists, values)
}

func (s *shardWorker) applyListPopFront(cmd shardCommand) shardResult {
	return s.applyListPop(cmd, true)
}

func (s *shardWorker) applyListPopBack(cmd shardCommand) shardResult {
	return s.applyListPop(cmd, false)
}

func (s *shardWorker) applyListPop(cmd shardCommand, front bool) shardResult {
	values, ent, exists, ok, err := s.mutableList(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if !exists {
		return primitiveMiss()
	}
	index := 0
	if !front {
		index = values.Len() - 1
	}
	value, removed := values.RemoveAt(index)
	if !removed {
		return shardResult{primitive: PrimitiveResult{
			Record: ent.record(),
			Count:  checkedUint64(values.Len()),
		}}
	}
	result := s.writeListCollection(cmd, ent, true, values)
	result.primitive.Value = append([]byte(nil), value...)
	return result
}

func (s *shardWorker) applyListRange(cmd shardCommand) shardResult {
	values, ent, ok, err := s.readList(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Count:  checkedUint64(values.Len()),
		Values: listRangeValues(values, cmd.primitive),
	}}
}

func (s *shardWorker) mutableList(
	cmd shardCommand,
) (*collectionlist.List[[]byte], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionlist.NewList[[]byte](), nil, false, true, nil
	}
	values, err := decodeListCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return values, ent, true, true, nil
}

func (s *shardWorker) readList(cmd shardCommand) (*collectionlist.List[[]byte], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	values, err := decodeListCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return values, ent, true, nil
}

func (s *shardWorker) writeListCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	values *collectionlist.List[[]byte],
) shardResult {
	value, err := encodeListCollection(values)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(values.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(values.Len()))}
}

func listRangeValues(values *collectionlist.List[[]byte], request PrimitiveRequest) [][]byte {
	items := values.Values()
	if request.Reverse {
		slices.Reverse(items)
	}
	start := listStartIndex(request.Start, len(items))
	if start >= len(items) {
		return nil
	}
	return cloneListValues(items[start:listEndIndex(start, request.Limit, len(items))])
}

func listStartIndex(start int64, length int) int {
	if start < 0 {
		return 0
	}
	if start > int64(length) {
		return length
	}
	return int(start)
}

func listEndIndex(start int, limit uint64, length int) int {
	if limit == 0 {
		return length
	}
	end := start
	for end < length && limit > 0 {
		end++
		limit--
	}
	return end
}

func cloneListValues(values [][]byte) [][]byte {
	out := make([][]byte, 0, len(values))
	for index := range values {
		out = append(out, append([]byte(nil), values[index]...))
	}
	return out
}
