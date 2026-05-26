package redis

import (
	"context"
	"strconv"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleSetBit(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 4 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityBitmap); wrong != nil {
		return *wrong
	}
	offset, ok := parseNonNegativeInt64(args[2])
	if !ok {
		return integerError()
	}
	bit, ok := parseBit(args[3])
	if !ok {
		return integerError()
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:         cache.PrimitiveBitmapSetBit,
		Key:          s.key(state, entityBitmap, key),
		Delta:        offset,
		InitialValue: bit,
	})
	if err != nil {
		return serviceError(err)
	}
	if result.Bool {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleGetBit(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityBitmap); wrong != nil {
		return *wrong
	}
	offset, ok := parseNonNegativeInt64(args[2])
	if !ok {
		return integerError()
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:  cache.PrimitiveBitmapGetBit,
		Key:   s.key(state, entityBitmap, key),
		Delta: offset,
	})
	if err != nil {
		return serviceError(err)
	}
	if result.Bool {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleBitCount(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 && len(args) != 4 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityBitmap); wrong != nil {
		return *wrong
	}
	request := cache.PrimitiveRequest{
		Kind: cache.PrimitiveBitmapBitCount,
		Key:  s.key(state, entityBitmap, key),
	}
	if len(args) == 4 {
		start, limit, ok := parseBitCountRange(args[2], args[3])
		if !ok {
			return integerError()
		}
		request.Start = start
		request.Limit = limit
	}
	result, err := state.activeService(s).Primitive(ctx, request)
	if err != nil {
		return serviceError(err)
	}
	return integerCount(result.Count)
}

func parseBit(raw respArg) (int64, bool) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	return value, err == nil && (value == 0 || value == 1)
}

func parseNonNegativeInt64(raw respArg) (int64, bool) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	return value, err == nil && value >= 0
}

func parseBitCountRange(startRaw, endRaw respArg) (int64, uint64, bool) {
	start, ok := parseNonNegativeInt64(startRaw)
	if !ok {
		return 0, 0, false
	}
	end, ok := parseNonNegativeInt64(endRaw)
	if !ok || end < start {
		return 0, 0, false
	}
	startBit := start * 8
	limit := checkedRedisUint64(end-start+1) * 8
	return startBit, limit, true
}

func checkedRedisUint64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func checkedRedisUint64FromInt(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
