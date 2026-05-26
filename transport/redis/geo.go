package redis

import (
	"context"
	"strconv"
	"strings"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleGeoAdd(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 5 || (len(args)-2)%3 != 0 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityGeo); wrong != nil {
		return *wrong
	}

	var added int64
	for index := 2; index < len(args); index += 3 {
		longitude, latitude, errResp := parseGeoPoint(args[index], args[index+1])
		if errResp != nil {
			return *errResp
		}
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:     cache.PrimitiveGeoAdd,
			Key:      s.key(state, entityGeo, key),
			Member:   string(args[index+2]),
			Score:    longitude,
			MinScore: latitude,
		})
		if err != nil {
			return serviceError(err)
		}
		if result.Bool {
			added++
		}
	}
	return integerValue(added)
}

func (s *Server) handleGeoDist(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 4 && len(args) != 5 {
		return syntaxError()
	}
	unit, ok := geoUnit(args, 4)
	if !ok {
		return errorString("ERR unsupported unit provided")
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityGeo); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:   cache.PrimitiveGeoDist,
		Key:    s.key(state, entityGeo, key),
		Member: string(args[2]),
		Field:  string(args[3]),
	})
	if err != nil {
		return serviceError(err)
	}
	if len(result.ScoredMembers) == 0 {
		return nullBulkString()
	}
	return bulkText(formatGeoDistance(result.ScoredMembers[0].Score, unit))
}

func (s *Server) handleGeoRadius(ctx context.Context, state *session, args []respArg) respValue {
	request, withDist, errResp := parseGeoRadius(args)
	if errResp != nil {
		return *errResp
	}
	if wrong := s.wrongTypeFor(ctx, state, request.key, entityGeo); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:     cache.PrimitiveGeoRadius,
		Key:      s.key(state, entityGeo, request.key),
		Score:    request.longitude,
		MinScore: request.latitude,
		MaxScore: request.radiusMeters,
		Limit:    request.limit,
	})
	if err != nil {
		return serviceError(err)
	}
	return geoRadiusResponse(result.ScoredMembers, request.unit, withDist)
}

type geoRadiusRequest struct {
	key          string
	longitude    float64
	latitude     float64
	radiusMeters float64
	unit         string
	limit        uint64
}

func parseGeoRadius(args []respArg) (geoRadiusRequest, bool, *respValue) {
	if len(args) < 6 {
		err := syntaxError()
		return geoRadiusRequest{}, false, &err
	}
	longitude, latitude, errResp := parseGeoPoint(args[2], args[3])
	if errResp != nil {
		return geoRadiusRequest{}, false, errResp
	}
	radius, err := strconv.ParseFloat(string(args[4]), 64)
	if err != nil || radius < 0 {
		err := errorString("ERR radius is not a valid float")
		return geoRadiusRequest{}, false, &err
	}
	unit := strings.ToLower(string(args[5]))
	multiplier, ok := geoUnitMultiplier(unit)
	if !ok {
		err := errorString("ERR unsupported unit provided")
		return geoRadiusRequest{}, false, &err
	}
	request := geoRadiusRequest{
		key:          string(args[1]),
		longitude:    longitude,
		latitude:     latitude,
		radiusMeters: radius * multiplier,
		unit:         unit,
	}
	withDist, errResp := applyGeoRadiusOptions(args[6:], &request)
	return request, withDist, errResp
}

func applyGeoRadiusOptions(args []respArg, request *geoRadiusRequest) (bool, *respValue) {
	var withDist bool
	for index := 0; index < len(args); {
		switch strings.ToUpper(string(args[index])) {
		case "WITHDIST":
			withDist = true
			index++
		case "COUNT":
			if index+1 >= len(args) {
				err := syntaxError()
				return false, &err
			}
			limit, ok := parseNonNegativeInt64(args[index+1])
			if !ok {
				err := integerError()
				return false, &err
			}
			request.limit = checkedRedisUint64(limit)
			index += 2
		default:
			err := syntaxError()
			return false, &err
		}
	}
	return withDist, nil
}

func parseGeoPoint(longitudeRaw, latitudeRaw respArg) (float64, float64, *respValue) {
	longitude, err := strconv.ParseFloat(string(longitudeRaw), 64)
	if err != nil {
		errResp := errorString("ERR longitude is not a valid float")
		return 0, 0, &errResp
	}
	latitude, err := strconv.ParseFloat(string(latitudeRaw), 64)
	if err != nil {
		errResp := errorString("ERR latitude is not a valid float")
		return 0, 0, &errResp
	}
	return longitude, latitude, nil
}

func geoUnit(args []respArg, index int) (string, bool) {
	if len(args) <= index {
		return "m", true
	}
	unit := strings.ToLower(string(args[index]))
	_, ok := geoUnitMultiplier(unit)
	return unit, ok
}

func geoUnitMultiplier(unit string) (float64, bool) {
	switch unit {
	case "m":
		return 1, true
	case "km":
		return 1000, true
	case "mi":
		return 1609.344, true
	case "ft":
		return 0.3048, true
	default:
		return 0, false
	}
}

func geoRadiusResponse(members []cache.ScoredMember, unit string, withDist bool) respValue {
	items := make([]respValue, 0, len(members))
	for index := range members {
		if !withDist {
			items = append(items, bulkText(members[index].Member))
			continue
		}
		items = append(items, arrayValue(
			bulkText(members[index].Member),
			bulkText(formatGeoDistance(members[index].Score, unit)),
		))
	}
	return arrayValue(items...)
}

func formatGeoDistance(meters float64, unit string) string {
	multiplier, ok := geoUnitMultiplier(unit)
	if !ok || multiplier == 0 {
		multiplier = 1
	}
	return strconv.FormatFloat(meters/multiplier, 'f', 4, 64)
}
