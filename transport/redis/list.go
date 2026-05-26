package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleLPush(ctx context.Context, state *session, args []respArg) respValue {
	return s.pushList(ctx, state, args, cache.PrimitiveListPushFront)
}

func (s *Server) handleRPush(ctx context.Context, state *session, args []respArg) respValue {
	return s.pushList(ctx, state, args, cache.PrimitiveListPushBack)
}

func (s *Server) pushList(
	ctx context.Context,
	state *session,
	args []respArg,
	kind cache.PrimitiveKind,
) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityList); wrong != nil {
		return *wrong
	}

	var result cache.PrimitiveResult
	for index := 2; index < len(args); index++ {
		next, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:  kind,
			Key:   s.key(state, entityList, key),
			Value: args[index],
		})
		if err != nil {
			return serviceError(err)
		}
		result = next
	}
	return integerCount(result.Count)
}

func (s *Server) handleLPop(ctx context.Context, state *session, args []respArg) respValue {
	return s.popList(ctx, state, args, cache.PrimitiveListPopFront)
}

func (s *Server) handleRPop(ctx context.Context, state *session, args []respArg) respValue {
	return s.popList(ctx, state, args, cache.PrimitiveListPopBack)
}

func (s *Server) popList(
	ctx context.Context,
	state *session,
	args []respArg,
	kind cache.PrimitiveKind,
) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityList); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: kind,
		Key:  s.key(state, entityList, key),
	})
	if err != nil {
		return serviceError(err)
	}
	if !result.Found || len(result.Value) == 0 {
		return nullBulkString()
	}
	return bulkString(result.Value)
}

func (s *Server) handleLRange(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 4 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityList); wrong != nil {
		return *wrong
	}
	start, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return integerError()
	}
	stop, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil {
		return integerError()
	}
	result, err := s.listValues(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	values := sliceRedisRange(result.Values, start, stop)
	items := make([]respValue, 0, len(values))
	for index := range values {
		items = append(items, bulkString(values[index]))
	}
	return arrayValue(items...)
}

func (s *Server) handleLLen(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityList); wrong != nil {
		return *wrong
	}
	result, err := s.listValues(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	return integerCount(result.Count)
}

func (s *Server) listValues(ctx context.Context, state *session, key string) (cache.PrimitiveResult, error) {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: cache.PrimitiveListRange,
		Key:  s.key(state, entityList, key),
	})
	if err != nil {
		return cache.PrimitiveResult{}, fmt.Errorf("read redis list values: %w", err)
	}
	return result, nil
}
