package engine

func (s *shardWorker) estimateMapPrimitive(cmd shardCommand) shardResult {
	fields, ent, exists, ok, err := s.mutableMap(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if cmd.primitive.Kind == PrimitiveMapSet {
		return estimateMapSet(cmd, fields, ent, exists)
	}
	if cmd.primitive.Kind == PrimitiveMapDelete {
		return estimateMapDelete(cmd, fields, ent, exists)
	}
	return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown map kind")}
}

func estimateMapSet(
	cmd shardCommand,
	fields mapCollection,
	ent *entry,
	exists bool,
) shardResult {
	if sameMapField(fields, cmd.primitive.Field, cmd.primitive.Value) && exists {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	fields.Set(cmd.primitive.Field, append([]byte(nil), cmd.primitive.Value...))
	return estimateMapCollection(cmd, fields, ent, exists)
}

func estimateMapDelete(
	cmd shardCommand,
	fields mapCollection,
	ent *entry,
	exists bool,
) shardResult {
	if !exists {
		return shardResult{estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if !fields.Delete(cmd.primitive.Field) {
		return shardResult{estimate: unchangedPrimitiveEstimate(cmd, ent)}
	}
	return estimateMapCollection(cmd, fields, ent, true)
}

func estimateMapCollection(cmd shardCommand, fields mapCollection, ent *entry, exists bool) shardResult {
	value, err := encodeMapCollection(fields)
	if err != nil {
		return shardResult{err: err}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
