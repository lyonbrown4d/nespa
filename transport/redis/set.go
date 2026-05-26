package redis

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleSAdd(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entitySet); wrong != nil {
		return *wrong
	}

	var added int64
	for index := 2; index < len(args); index++ {
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:   cache.PrimitiveSetAdd,
			Key:    s.key(state, entitySet, key),
			Member: string(args[index]),
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

func (s *Server) handleSRem(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entitySet); wrong != nil {
		return *wrong
	}

	var removed int64
	for index := 2; index < len(args); index++ {
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:   cache.PrimitiveSetRemove,
			Key:    s.key(state, entitySet, key),
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

func (s *Server) handleSIsMember(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entitySet); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:   cache.PrimitiveSetContains,
		Key:    s.key(state, entitySet, key),
		Member: string(args[2]),
	})
	if err != nil {
		return serviceError(err)
	}
	if result.Bool {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleSMembers(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entitySet); wrong != nil {
		return *wrong
	}
	result, err := s.setMembers(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	items := make([]respValue, 0, len(result.Members))
	for index := range result.Members {
		items = append(items, bulkText(result.Members[index]))
	}
	return arrayValue(items...)
}

func (s *Server) handleSCard(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entitySet); wrong != nil {
		return *wrong
	}
	result, err := s.setMembers(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	return integerCount(result.Count)
}

func (s *Server) setMembers(ctx context.Context, state *session, key string) (cache.PrimitiveResult, error) {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: cache.PrimitiveSetMembers,
		Key:  s.key(state, entitySet, key),
	})
	if err != nil {
		return cache.PrimitiveResult{}, fmt.Errorf("read redis set members: %w", err)
	}
	return result, nil
}
