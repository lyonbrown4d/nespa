package engine

func (s *shardWorker) estimateGeoPrimitive(cmd shardCommand) shardResult {
	points, ent, exists, ok, err := s.mutableGeo(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	points.Set(cmd.primitive.Member, GeoPoint{
		Longitude: cmd.primitive.Score,
		Latitude:  cmd.primitive.MinScore,
	})
	value, err := encodeGeoCollection(points)
	if err != nil {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	return shardResult{estimate: estimateWriteCost(cmd, ent, exists, value)}
}
