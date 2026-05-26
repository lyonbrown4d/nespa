package engine

func (s *shardWorker) estimateBitmapPrimitive(cmd shardCommand) shardResult {
	bits, ent, exists, ok, err := s.mutableBitmap(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	offset := int(cmd.primitive.Delta)
	next := cmd.primitive.InitialValue == 1
	if next {
		bits.Add(offset)
	} else {
		bits.Remove(offset)
	}
	value, err := encodeBitmapCollection(bits)
	if err != nil {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
