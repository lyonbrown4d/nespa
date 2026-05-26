package cache

import (
	"context"
	"fmt"
)

func (s *EngineService) Primitive(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	if request.Kind.Mutates() {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.primitiveLocked(ctx, request)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.primitiveLocked(ctx, request)
}

func (s *EngineService) primitiveLocked(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	if err := s.admitPrimitive(ctx, request); err != nil {
		return PrimitiveResult{}, err
	}
	return s.executePrimitive(ctx, request)
}

func (s *EngineService) executePrimitive(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	result, err := s.engine.Primitive(ctx, request)
	if err != nil {
		return PrimitiveResult{}, fmt.Errorf("execute engine primitive: %w", err)
	}
	return result, nil
}
