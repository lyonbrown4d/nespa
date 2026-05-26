package engine

import (
	"math"
	"sort"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

const earthRadiusMeters = 6371008.8

func (s *shardWorker) applyGeoAdd(cmd shardCommand) shardResult {
	points, ent, exists, ok, err := s.mutableGeo(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	_, hasCurrent := points.Get(cmd.primitive.Member)
	points.Set(cmd.primitive.Member, GeoPoint{
		Longitude: cmd.primitive.Score,
		Latitude:  cmd.primitive.MinScore,
	})
	result := s.writeGeoCollection(cmd, ent, exists, points)
	result.primitive.Bool = !hasCurrent
	return result
}

func (s *shardWorker) applyGeoDist(cmd shardCommand) shardResult {
	points, ent, ok, err := s.readGeo(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	left, leftOK := points.Get(cmd.primitive.Member)
	right, rightOK := points.Get(cmd.primitive.Field)
	if !leftOK || !rightOK {
		return shardResult{primitive: PrimitiveResult{Record: ent.record(), Found: false}}
	}
	return shardResult{primitive: PrimitiveResult{
		Record: ent.record(),
		Found:  true,
		Count:  1,
		ScoredMembers: []ScoredMember{{
			Member: cmd.primitive.Field,
			Score:  geoDistanceMeters(left, right),
		}},
	}}
}

func (s *shardWorker) applyGeoRadius(cmd shardCommand) shardResult {
	points, ent, ok, err := s.readGeo(cmd)
	if err != nil || !ok {
		return shardResult{err: err}
	}
	center := GeoPoint{Longitude: cmd.primitive.Score, Latitude: cmd.primitive.MinScore}
	return shardResult{primitive: PrimitiveResult{
		Record:        ent.record(),
		Found:         true,
		Count:         checkedUint64(points.Len()),
		ScoredMembers: geoRadius(points, center, cmd.primitive.MaxScore, cmd.primitive.Limit),
	}}
}

func (s *shardWorker) mutableGeo(
	cmd shardCommand,
) (*collectionmapping.Map[string, GeoPoint], *entry, bool, bool, error) {
	ent, exists, ok := s.mutablePrimitiveEntry(cmd)
	if !ok {
		return nil, nil, exists, false, nil
	}
	if !exists {
		return collectionmapping.NewMap[string, GeoPoint](), nil, false, true, nil
	}
	points, err := decodeGeoCollection(ent.value)
	if err != nil {
		return nil, nil, true, false, err
	}
	return points, ent, true, true, nil
}

func (s *shardWorker) readGeo(
	cmd shardCommand,
) (*collectionmapping.Map[string, GeoPoint], *entry, bool, error) {
	ent, ok := s.readPrimitiveEntry(cmd)
	if !ok {
		return nil, nil, false, nil
	}
	points, err := decodeGeoCollection(ent.value)
	if err != nil {
		return nil, nil, false, err
	}
	return points, ent, true, nil
}

func (s *shardWorker) writeGeoCollection(
	cmd shardCommand,
	ent *entry,
	exists bool,
	points *collectionmapping.Map[string, GeoPoint],
) shardResult {
	value, err := encodeGeoCollection(points)
	if err != nil {
		return shardResult{err: err}
	}
	if !exists {
		return shardResult{primitive: s.createPrimitiveEntry(cmd, value, checkedUint64(points.Len()))}
	}
	return shardResult{primitive: s.writePrimitiveEntry(cmd, ent, value, checkedUint64(points.Len()))}
}

func geoRadius(
	points *collectionmapping.Map[string, GeoPoint],
	center GeoPoint,
	radiusMeters float64,
	limit uint64,
) []ScoredMember {
	items := make([]ScoredMember, 0, points.Len())
	points.Range(func(member string, point GeoPoint) bool {
		distance := geoDistanceMeters(center, point)
		if distance <= radiusMeters {
			items = append(items, ScoredMember{Member: member, Score: distance})
		}
		return true
	})
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return items[i].Member < items[j].Member
		}
		return items[i].Score < items[j].Score
	})
	return limitScoredMembers(items, limit)
}

func geoDistanceMeters(left, right GeoPoint) float64 {
	leftLat := degreesToRadians(left.Latitude)
	rightLat := degreesToRadians(right.Latitude)
	deltaLat := degreesToRadians(right.Latitude - left.Latitude)
	deltaLon := degreesToRadians(right.Longitude - left.Longitude)
	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(leftLat)*math.Cos(rightLat)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	return earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func degreesToRadians(value float64) float64 {
	return value * math.Pi / 180
}
