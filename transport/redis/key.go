package redis

import (
	"context"
	"strconv"

	"github.com/lyonbrown4d/nespa/cache"
)

func (s *Server) key(state *session, entity, key string) cache.Key {
	return cache.Key{
		Namespace: state.namespace,
		Space:     s.spaceName(state.db),
		Entity:    entity,
		Key:       key,
	}
}

func (s *Server) typeOf(ctx context.Context, state *session, key string) (redisType, bool) {
	for index := range redisEntities {
		typ := redisEntities[index]
		_, found, err := state.activeService(s).Get(ctx, s.key(state, typ.entity, key), cache.GetOptions{})
		if err == nil && found {
			return typ, true
		}
	}
	return redisType{}, false
}

func (s *Server) existsAny(ctx context.Context, state *session, key string) bool {
	_, found := s.typeOf(ctx, state, key)
	return found
}

func (s *Server) deleteAny(ctx context.Context, state *session, key string) bool {
	var deleted bool
	for index := range redisEntities {
		removed, _, err := state.activeService(s).Delete(ctx, s.key(state, redisEntities[index].entity, key), cache.DeleteOptions{})
		if err == nil && removed {
			deleted = true
		}
	}
	return deleted
}

func (s *Server) deleteNonStringTypes(ctx context.Context, state *session, key string) {
	for index := range redisEntities {
		entity := redisEntities[index].entity
		if entity == entityString {
			continue
		}
		if _, _, err := state.activeService(s).Delete(ctx, s.key(state, entity, key), cache.DeleteOptions{}); err != nil {
			continue
		}
	}
}

func (s *Server) wrongTypeFor(ctx context.Context, state *session, key, expectedEntity string) *respValue {
	typ, found := s.typeOf(ctx, state, key)
	if !found || typ.entity == expectedEntity {
		return nil
	}
	out := wrongType()
	return &out
}

func parsePositiveInt64(raw []byte) (int64, bool) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	return value, err == nil && value > 0
}
