package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleZAdd(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 4 || len(args)%2 != 0 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityScoredSet); wrong != nil {
		return *wrong
	}

	var added int64
	for index := 2; index < len(args); index += 2 {
		score, err := strconv.ParseFloat(string(args[index]), 64)
		if err != nil {
			return errorString("ERR value is not a valid float")
		}
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:   cache.PrimitiveScoredSetPut,
			Key:    s.key(state, entityScoredSet, key),
			Member: string(args[index+1]),
			Score:  score,
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

func (s *Server) handleZRem(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityScoredSet); wrong != nil {
		return *wrong
	}

	var removed int64
	for index := 2; index < len(args); index++ {
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:   cache.PrimitiveScoredSetRemove,
			Key:    s.key(state, entityScoredSet, key),
			Member: string(args[index]),
		})
		if err != nil {
			return serviceError(err)
		}
		if result.Bool {
			removed++
		}
	}
	return integerValue(removed)
}

func (s *Server) handleZRange(ctx context.Context, state *session, args []respArg) respValue {
	key, start, stop, withScores, errResp := parseZRangeArgs(args)
	if errResp != nil {
		return *errResp
	}
	if wrong := s.wrongTypeFor(ctx, state, key, entityScoredSet); wrong != nil {
		return *wrong
	}

	result, err := s.scoredValues(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	return zrangeResponse(result, start, stop, withScores)
}

func parseZRangeArgs(args []respArg) (string, int64, int64, bool, *respValue) {
	if len(args) != 4 && len(args) != 5 {
		err := syntaxError()
		return "", 0, 0, false, &err
	}
	start, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		errResp := integerError()
		return "", 0, 0, false, &errResp
	}
	stop, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil {
		errResp := integerError()
		return "", 0, 0, false, &errResp
	}
	withScores := len(args) == 5 && strings.EqualFold(string(args[4]), "WITHSCORES")
	if len(args) == 5 && !withScores {
		errResp := syntaxError()
		return "", 0, 0, false, &errResp
	}
	return string(args[1]), start, stop, withScores, nil
}

func zrangeResponse(result cache.PrimitiveResult, start, stop int64, withScores bool) respValue {
	members := sliceRedisRange(result.ScoredMembers, start, stop)
	items := make([]respValue, 0, len(members))
	if withScores {
		items = make([]respValue, 0, len(members)*2)
	}
	for index := range members {
		items = append(items, bulkText(members[index].Member))
		if withScores {
			items = append(items, bulkText(strconv.FormatFloat(members[index].Score, 'f', -1, 64)))
		}
	}
	return arrayValue(items...)
}

func (s *Server) handleZCard(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityScoredSet); wrong != nil {
		return *wrong
	}
	result, err := s.scoredValues(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	return integerCount(result.Count)
}

func (s *Server) scoredValues(ctx context.Context, state *session, key string) (cache.PrimitiveResult, error) {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: cache.PrimitiveScoredSetRange,
		Key:  s.key(state, entityScoredSet, key),
	})
	if err != nil {
		return cache.PrimitiveResult{}, fmt.Errorf("read redis zset values: %w", err)
	}
	return result, nil
}
