package redis

import (
	"bytes"
	"context"
	"fmt"

	"github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) handlePFAdd(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 3 {
		return syntaxError()
	}
	key := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, key, entityHLL); wrong != nil {
		return *wrong
	}
	var changed int64
	for index := 2; index < len(args); index++ {
		result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
			Kind:   cache.PrimitiveHLLAdd,
			Key:    s.key(state, entityHLL, key),
			Member: string(args[index]),
		})
		if err != nil {
			return serviceError(err)
		}
		if result.Bool {
			changed = 1
		}
	}
	return integerValue(changed)
}

func (s *Server) handlePFCount(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	if len(args) == 2 {
		key := string(args[1])
		if wrong := s.wrongTypeFor(ctx, state, key, entityHLL); wrong != nil {
			return *wrong
		}
		result, err := s.hllMembers(ctx, state, key)
		if err != nil {
			return serviceError(err)
		}
		return integerCount(result.Count)
	}
	hashes, errResp := s.hllUnion(ctx, state, args[1:])
	if errResp != nil {
		return *errResp
	}
	return integerCount(checkedRedisUint64FromInt(hashes.Len()))
}

func (s *Server) handlePFMerge(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) < 2 {
		return syntaxError()
	}
	dest := string(args[1])
	if wrong := s.wrongTypeFor(ctx, state, dest, entityHLL); wrong != nil {
		return *wrong
	}
	hashes, errResp := s.hllUnion(ctx, state, args[2:])
	if errResp != nil {
		return *errResp
	}
	_, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind:  cache.PrimitiveHLLMerge,
		Key:   s.key(state, entityHLL, dest),
		Value: encodeHLLHashValues(hashes.Values()),
	})
	if err != nil {
		return serviceError(err)
	}
	return simpleString("OK")
}

func (s *Server) hllUnion(ctx context.Context, state *session, keys []respArg) (*set.Set[string], *respValue) {
	hashes := set.NewSet[string]()
	for index := range keys {
		key := string(keys[index])
		if wrong := s.wrongTypeFor(ctx, state, key, entityHLL); wrong != nil {
			return nil, wrong
		}
		result, err := s.hllMembers(ctx, state, key)
		if err != nil {
			errResp := serviceError(err)
			return nil, &errResp
		}
		hashes.Add(result.Members...)
	}
	return hashes, nil
}

func (s *Server) hllMembers(ctx context.Context, state *session, key string) (cache.PrimitiveResult, error) {
	result, err := state.activeService(s).Primitive(ctx, cache.PrimitiveRequest{
		Kind: cache.PrimitiveHLLMembers,
		Key:  s.key(state, entityHLL, key),
	})
	if err != nil {
		return cache.PrimitiveResult{}, fmt.Errorf("read hll members: %w", err)
	}
	return result, nil
}

func encodeHLLHashValues(values []string) []byte {
	raw := make([][]byte, 0, len(values))
	for index := range values {
		raw = append(raw, []byte(values[index]))
	}
	return bytes.Join(raw, []byte{0})
}
