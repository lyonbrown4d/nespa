package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/lyonbrown4d/nespa/internal/node/engine"
)

type Key = engine.Key
type Record = engine.Record
type Stats = engine.Stats
type ShardStats = engine.ShardStats
type SpaceStats = engine.SpaceStats
type EvictResult = engine.EvictResult

var ErrQuotaExceeded = errors.New("cache: quota exceeded")

type QuotaConfig struct {
	DefaultNamespaceMemoryBytes uint64
	DefaultSpaceMemoryBytes     uint64
	Namespaces                  map[string]NamespaceQuota
	Spaces                      map[SpaceRef]SpaceQuota
}

type NamespaceQuota struct {
	MemoryBytes uint64
}

type SpaceQuota struct {
	MemoryBytes uint64
}

type SpaceRef struct {
	Namespace string
	Space     string
}

type SetOptions struct {
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
}

type GetOptions struct {
	NamespaceVersion uint64
	SpaceVersion     uint64
}

type TouchOptions struct {
	TTL time.Duration
}

type Service interface {
	Set(context.Context, Key, []byte, SetOptions) (Record, error)
	Get(context.Context, Key, GetOptions) (Record, bool, error)
	Delete(context.Context, Key) (bool, error)
	Exists(context.Context, Key, GetOptions) (bool, error)
	Touch(context.Context, Key, TouchOptions) (bool, error)
	BatchSet(context.Context, []SetRequest) ([]Record, error)
	BatchGet(context.Context, []GetRequest) ([]GetResult, error)
	Stats(context.Context) (Stats, error)
	Evict(context.Context, EvictRequest) (EvictResult, error)
}

type SetRequest struct {
	Key     Key
	Value   []byte
	Options SetOptions
}

type GetRequest struct {
	Key     Key
	Options GetOptions
}

type GetResult struct {
	Record Record
	Found  bool
}

type EvictRequest struct {
	Namespace   string
	Space       string
	TargetBytes uint64
	Exclude     Key
}

type EngineService struct {
	engine engine.Engine
	quota  QuotaConfig
	mu     sync.Mutex
}

type Option func(*EngineService)

func WithQuota(cfg QuotaConfig) Option {
	return func(s *EngineService) {
		s.quota = cfg
	}
}

func NewService(eng engine.Engine, opts ...Option) *EngineService {
	svc := &EngineService{engine: eng}
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

func Module(svc Service) dix.Module {
	return dix.NewModule("node.cache",
		dix.WithModuleProviders(
			dix.Value[Service](svc),
		),
	)
}

func (s *EngineService) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.admitSet(ctx, key, value); err != nil {
		return Record{}, err
	}

	return s.engine.Set(ctx, key, value, engine.SetOptions{
		TTL:              opts.TTL,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
}

func (s *EngineService) Get(ctx context.Context, key Key, opts GetOptions) (Record, bool, error) {
	return s.engine.Get(ctx, key, engine.GetOptions{
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
}

func (s *EngineService) Delete(ctx context.Context, key Key) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.engine.Delete(ctx, key)
}

func (s *EngineService) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	return s.engine.Exists(ctx, key, engine.GetOptions{
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
}

func (s *EngineService) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	return s.engine.Touch(ctx, key, engine.TouchOptions{TTL: opts.TTL})
}

func (s *EngineService) BatchSet(ctx context.Context, requests []SetRequest) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]Record, 0, len(requests))
	for _, request := range requests {
		if err := s.admitSet(ctx, request.Key, request.Value); err != nil {
			return records, err
		}
		record, err := s.engine.Set(ctx, request.Key, request.Value, engine.SetOptions{
			TTL:              request.Options.TTL,
			NamespaceVersion: request.Options.NamespaceVersion,
			SpaceVersion:     request.Options.SpaceVersion,
		})
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *EngineService) BatchGet(ctx context.Context, requests []GetRequest) ([]GetResult, error) {
	results := make([]GetResult, 0, len(requests))
	for _, request := range requests {
		record, found, err := s.Get(ctx, request.Key, request.Options)
		if err != nil {
			return results, err
		}
		results = append(results, GetResult{Record: record, Found: found})
	}
	return results, nil
}

func (s *EngineService) Stats(ctx context.Context) (Stats, error) {
	return s.engine.Stats(ctx)
}

func (s *EngineService) Evict(ctx context.Context, request EvictRequest) (EvictResult, error) {
	return s.engine.Evict(ctx, engine.EvictOptions{
		Namespace:     request.Namespace,
		Space:         request.Space,
		TargetBytes:   request.TargetBytes,
		Exclude:       request.Exclude,
		ExcludeActive: request.Exclude.Key != "",
	})
}

func (s *EngineService) admitSet(ctx context.Context, key Key, value []byte) error {
	if s.quota.DefaultNamespaceMemoryBytes == 0 && s.quota.DefaultSpaceMemoryBytes == 0 &&
		len(s.quota.Namespaces) == 0 && len(s.quota.Spaces) == 0 {
		return nil
	}

	nextCost := engine.EstimateCost(key, value)
	current, found, err := s.engine.Get(ctx, key, engine.GetOptions{})
	if err != nil {
		return err
	}
	var oldCost uint64
	if found {
		oldCost = current.CostBytes
	}
	if nextCost <= oldCost {
		return nil
	}
	delta := nextCost - oldCost

	stats, err := s.engine.Stats(ctx)
	if err != nil {
		return err
	}
	nsUsage, spaceUsage := usageFor(stats, key.Namespace, key.Space)

	if limit := s.spaceLimit(key.Namespace, key.Space); limit > 0 && spaceUsage+delta > limit {
		needed := spaceUsage + delta - limit
		evicted, err := s.Evict(ctx, EvictRequest{
			Namespace:   key.Namespace,
			Space:       key.Space,
			TargetBytes: needed,
			Exclude:     key,
		})
		if err != nil {
			return err
		}
		spaceUsage -= minUint64(spaceUsage, evicted.FreedBytes)
		nsUsage -= minUint64(nsUsage, evicted.FreedBytes)
		if spaceUsage+delta > limit {
			return fmt.Errorf("%w: space %q/%q memory %d + %d > %d", ErrQuotaExceeded, key.Namespace, key.Space, spaceUsage, delta, limit)
		}
	}
	if limit := s.namespaceLimit(key.Namespace); limit > 0 && nsUsage+delta > limit {
		return fmt.Errorf("%w: namespace %q memory %d + %d > %d", ErrQuotaExceeded, key.Namespace, nsUsage, delta, limit)
	}
	return nil
}

func (s *EngineService) namespaceLimit(namespace string) uint64 {
	if quota, ok := s.quota.Namespaces[namespace]; ok {
		return quota.MemoryBytes
	}
	return s.quota.DefaultNamespaceMemoryBytes
}

func (s *EngineService) spaceLimit(namespace, space string) uint64 {
	if quota, ok := s.quota.Spaces[SpaceRef{Namespace: namespace, Space: space}]; ok {
		return quota.MemoryBytes
	}
	return s.quota.DefaultSpaceMemoryBytes
}

func usageFor(stats Stats, namespace, space string) (namespaceMemoryBytes, spaceMemoryBytes uint64) {
	for _, item := range stats.Spaces {
		if item.Namespace != namespace {
			continue
		}
		namespaceMemoryBytes += item.MemoryBytes
		if item.Space == space {
			spaceMemoryBytes += item.MemoryBytes
		}
	}
	return namespaceMemoryBytes, spaceMemoryBytes
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
