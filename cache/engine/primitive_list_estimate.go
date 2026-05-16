package engine

func (s *shardWorker) estimateListPrimitive(cmd shardCommand) shardResult {
	values, ent, exists, ok, err := s.mutableList(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	return estimateListMutation(cmd, values, ent, exists)
}

func estimateListMutation(cmd shardCommand, values listCollection, ent *entry, exists bool) shardResult {
	if cmd.primitive.Kind == PrimitiveListPushFront {
		return estimateListPush(cmd, values, ent, exists, true)
	}
	if cmd.primitive.Kind == PrimitiveListPushBack {
		return estimateListPush(cmd, values, ent, exists, false)
	}
	if cmd.primitive.Kind == PrimitiveListPopFront {
		return estimateListPop(cmd, values, ent, exists, true)
	}
	if cmd.primitive.Kind == PrimitiveListPopBack {
		return estimateListPop(cmd, values, ent, exists, false)
	}
	return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown list kind")}
}

func estimateListPush(cmd shardCommand, values listCollection, ent *entry, exists, front bool) shardResult {
	value := append([]byte(nil), cmd.primitive.Value...)
	if front {
		values.AddAt(0, value)
	} else {
		values.Add(value)
	}
	return estimateListCollection(cmd, values, ent, exists)
}

func estimateListPop(cmd shardCommand, values listCollection, ent *entry, exists, front bool) shardResult {
	if !exists {
		return shardResult{estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	index := 0
	if !front {
		index = values.Len() - 1
	}
	if _, removed := values.RemoveAt(index); !removed {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	return estimateListCollection(cmd, values, ent, true)
}

func estimateListCollection(cmd shardCommand, values listCollection, ent *entry, exists bool) shardResult {
	value, err := encodeListCollection(values)
	if err != nil {
		return shardResult{err: err}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
