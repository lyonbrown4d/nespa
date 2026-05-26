package redis

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handleHSet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 4 || len(args)%2 != 0 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}

	var added int64
	for index := 2; index < len(args); index += 2 {
		existed := s.hashFieldExists(ctx, state, key, string(args[index]))
		if errResp := s.mapSet(ctx, state, key, string(args[index]), args[index+1]); errResp != nil {
			return *errResp
		}
		if !existed {
			added++
		}
	}
	return integerValue(added)
}

func (s *Server) handleHGet(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:  cache.PrimitiveMapGet,
		Key:   s.key(state, entityHash, key),
		Field: string(args[2]),
	})
	if err != nil {
		return serviceError(err)
	}
	if !result.Found {
		return nullBulkString()
	}
	return bulkString(result.Value)
}

func (s *Server) handleHDel(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}

	var removed int64
	for index := 2; index < len(args); index++ {
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:  cache.PrimitiveMapDelete,
			Key:   s.key(state, entityHash, key),
			Field: string(args[index]),
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

func (s *Server) handleHGetAll(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}
	result, err := s.mapFields(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	items := make([]respValue, 0, len(result.Fields)*2)
	for index := range result.Fields {
		items = append(items, bulkText(result.Fields[index].Field), bulkString(result.Fields[index].Value))
	}
	return arrayValue(items...)
}

func (s *Server) handleHExists(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}
	if s.hashFieldExists(ctx, state, key, string(args[2])) {
		return integerValue(1)
	}
	return integerValue(0)
}

func (s *Server) handleHLen(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 2 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHash); wrong != nil {
		return *wrong
	}
	result, err := s.mapFields(ctx, state, key)
	if err != nil {
		return serviceError(err)
	}
	return integerCount(result.Count)
}

func (s *Server) mapSet(ctx context.Context, state *session, key, field string, value []byte) *respValue {
	_, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:  cache.PrimitiveMapSet,
		Key:   s.key(state, entityHash, key),
		Field: field,
		Value: value,
	})
	if err != nil {
		errResp := serviceError(err)
		return &errResp
	}
	return nil
}

func (s *Server) mapFields(ctx context.Context, state *session, key string) (cache.PrimitiveResult, error) {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: cache.PrimitiveMapGetAll,
		Key:  s.key(state, entityHash, key),
	})
	if err != nil {
		return cache.PrimitiveResult{}, fmt.Errorf("read redis hash fields: %w", err)
	}
	return result, nil
}

func (s *Server) hashFieldExists(ctx context.Context, state *session, key, field string) bool {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:  cache.PrimitiveMapGet,
		Key:   s.key(state, entityHash, key),
		Field: field,
	})
	return err == nil && result.Found
}
