package engine

import (
	"sort"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *shardWorker) applyScoredSetPut(cmd shardCommand) shardResult {
	scores, ent, exists, ok, err := s.mutableScoredSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	current, hasCurrent := scores.Get(cmd.primitive.Member)
	if exists && hasCurrent && current == cmd.primitive.Score {
		result := primitiveWriteResult(ent, checkedUint64(scores.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	scores.Set(cmd.primitive.Member, cmd.primitive.Score)
	result := s.writeScoredSetCollection(cmd, ent, exists, scores)
	result.primitive.Bool = !hasCurrent
	return result
}

func (s *shardWorker) applyScoredSetRemove(cmd shardCommand) shardResult {
	scores, ent, exists, ok, err := s.mutableScoredSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	if !exists {
		return primitiveMiss()
	}
	removed := scores.Delete(cmd.primitive.Member)
	if !removed {
		result := primitiveWriteResult(ent, checkedUint64(scores.Len()))
		result.Bool = false
		return shardResult{primitive: result}
	}
	result := s.writeScoredSetCollection(cmd, ent, true, scores)
	result.primitive.Bool = true
	return result
}

func (s *shardWorker) applyScoredSetRange(cmd shardCommand) shardResult {
	scores, ent, ok, err := s.readScoredSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	return shardResult{primitive: PrimitiveResult{
		Record:        ent.record(),
		Found:         true,
		Count:         checkedUint64(scores.Len()),
		ScoredMembers: scoredRange(scores, cmd.primitive),
	}}
}

func (s *shardWorker) mutableScoredSet(
	cmd shardCommand,
) (*collectionmapping.Map[string, float64], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionmapping.NewMap[string, float64](), nil, false, true, nil
	}
	scores, err := decodeScoredSetCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return scores, ent, true, true, nil
}

func (s *shardWorker) readScoredSet(
	cmd shardCommand,
) (*collectionmapping.Map[string, float64], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	scores, err := decodeScoredSetCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return scores, ent, true, nil
}

func (s *shardWorker) writeScoredSetCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	scores *collectionmapping.Map[string, float64],
) shardResult {
	value, err := encodeScoredSetCollection(scores)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(scores.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(scores.Len()))}
}

func scoredRange(scores *collectionmapping.Map[string, float64], request PrimitiveRequest) []ScoredMember {
	items := sortedScoredMembers(scores)
	items = filterScoredRange(items, request)
	if request.Reverse {
		reverseScoredMembers(items)
	}
	return limitScoredMembers(items, request.Limit)
}

func sortedScoredMembers(scores *collectionmapping.Map[string, float64]) []ScoredMember {
	items := make([]ScoredMember, 0, scores.Len())
	scores.Range(func(member string, score float64) bool {
		items = append(items, ScoredMember{Member: member, Score: score})
		return true
	})
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return items[i].Member < items[j].Member
		}
		return items[i].Score < items[j].Score
	})
	return items
}

func filterScoredRange(items []ScoredMember, request PrimitiveRequest) []ScoredMember {
	out := make([]ScoredMember, 0, len(items))
	for index := range items {
		item := items[index]
		if request.HasMinScore && item.Score < request.MinScore {
			continue
		}
		if request.HasMaxScore && item.Score > request.MaxScore {
			continue
		}
		out = append(out, item)
	}
	return out
}

func reverseScoredMembers(items []ScoredMember) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}

func limitScoredMembers(items []ScoredMember, limit uint64) []ScoredMember {
	if limit == 0 || checkedUint64(len(items)) <= limit {
		return items
	}
	end := 0
	for index := range items {
		if checkedUint64(index) >= limit {
			break
		}
		end = index + 1
	}
	return items[:end]
}
