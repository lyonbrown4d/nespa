package engine

func (s *shardWorker) estimateHLLPrimitive(cmd shardCommand) shardResult {
	hashes, ent, exists, ok, err := s.mutableHLL(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if cmd.primitive.Kind == PrimitiveHLLAdd {
		hashes.Add(hllHash(cmd.primitive.Member))
	}
	if cmd.primitive.Kind == PrimitiveHLLMerge {
		for _, hash := range decodeHLLHashes(cmd.primitive.Value) {
			hashes.Add(hash)
		}
	}
	value, err := encodeHLLCollection(hashes)
	if err != nil {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
