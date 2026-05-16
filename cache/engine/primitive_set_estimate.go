package engine

func (s *shardWorker) estimateSetPrimitive(cmd shardCommand) shardResult {
	members, ent, exists, ok, err := s.mutableSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if cmd.primitive.Kind == PrimitiveSetAdd {
		return estimateSetAdd(cmd, members, ent, exists)
	}
	if cmd.primitive.Kind == PrimitiveSetRemove {
		return estimateSetRemove(cmd, members, ent, exists)
	}
	return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown set kind")}
}

func estimateSetAdd(cmd shardCommand, members setCollection, ent *entry, exists bool) shardResult {
	if exists && members.Contains(cmd.primitive.Member) {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	members.Add(cmd.primitive.Member)
	return estimateSetCollection(cmd, members, ent, exists)
}

func estimateSetRemove(cmd shardCommand, members setCollection, ent *entry, exists bool) shardResult {
	if !exists {
		return shardResult{estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if !members.Remove(cmd.primitive.Member) {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	return estimateSetCollection(cmd, members, ent, true)
}

func estimateSetCollection(cmd shardCommand, members setCollection, ent *entry, exists bool) shardResult {
	value, err := encodeSetCollection(members)
	if err != nil {
		return shardResult{err: err}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
