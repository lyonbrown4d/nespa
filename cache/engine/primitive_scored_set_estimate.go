package engine

func (s *shardWorker) estimateScoredSetPrimitive(cmd shardCommand) shardResult {
	scores, ent, exists, ok, err := s.mutableScoredSet(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if cmd.primitive.Kind == PrimitiveScoredSetPut {
		return estimateScoredSetPut(cmd, scores, ent, exists)
	}
	if cmd.primitive.Kind == PrimitiveScoredSetRemove {
		return estimateScoredSetRemove(cmd, scores, ent, exists)
	}
	return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown scored set kind")}
}

func estimateScoredSetPut(
	cmd shardCommand,
	scores scoredSetCollection,
	ent *entry,
	exists bool,
) shardResult {
	current, hasCurrent := scores.Get(cmd.primitive.Member)
	if exists && hasCurrent && current == cmd.primitive.Score {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	scores.Set(cmd.primitive.Member, cmd.primitive.Score)
	return estimateScoredSetCollection(cmd, scores, ent, exists)
}

func estimateScoredSetRemove(
	cmd shardCommand,
	scores scoredSetCollection,
	ent *entry,
	exists bool,
) shardResult {
	if !exists {
		return shardResult{estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if !scores.Delete(cmd.primitive.Member) {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	return estimateScoredSetCollection(cmd, scores, ent, true)
}

func estimateScoredSetCollection(
	cmd shardCommand,
	scores scoredSetCollection,
	ent *entry,
	exists bool,
) shardResult {
	value, err := encodeScoredSetCollection(scores)
	if err != nil {
		return shardResult{err: err}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
