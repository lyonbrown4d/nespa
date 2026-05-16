// Package cache wraps the node storage engine with cache-level policy.
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/samber/oops"
)

type Key = engine.Key
type Record = engine.Record
type Stats = engine.Stats
type ShardStats = engine.ShardStats
type SpaceStats = engine.SpaceStats
type EvictResult = engine.EvictResult
type Snapshot = engine.Snapshot
type SnapshotEntry = engine.SnapshotEntry
type RangeOptions = engine.RangeOptions
type ImportResult = engine.ImportResult
type DeleteRangeResult = engine.DeleteRangeResult

var ErrQuotaExceeded = oops.Code("quota_exceeded").In("cache").New("cache: quota exceeded")

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
	ExpectedVersion  uint64
}

type GetOptions struct {
	NamespaceVersion uint64
	SpaceVersion     uint64
}

type TouchOptions struct {
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type AdjustOptions struct {
	Delta            int64
	InitialValue     int64
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type DeleteOptions struct {
	ExpectedVersion uint64
}

type Service interface {
	Set(context.Context, Key, []byte, SetOptions) (SetResult, error)
	Get(context.Context, Key, GetOptions) (Record, bool, error)
	Delete(context.Context, Key, DeleteOptions) (bool, bool, error)
	Exists(context.Context, Key, GetOptions) (bool, error)
	Touch(context.Context, Key, TouchOptions) (bool, error)
	Adjust(context.Context, Key, AdjustOptions) (SetResult, error)
	Primitive(context.Context, PrimitiveRequest) (PrimitiveResult, error)
	BatchPrimitive(context.Context, []PrimitiveRequest) ([]PrimitiveResult, error)
	BatchSet(context.Context, []SetRequest) ([]SetResult, error)
	BatchGet(context.Context, []GetRequest) ([]GetResult, error)
	BatchDelete(context.Context, []DeleteRequest) ([]DeleteResult, error)
	BatchExists(context.Context, []GetRequest) ([]ExistsResult, error)
	BatchTouch(context.Context, []TouchRequest) ([]TouchResult, error)
	Stats(context.Context) (Stats, error)
	Evict(context.Context, EvictRequest) (EvictResult, error)
	Export(context.Context, RangeOptions) (Snapshot, error)
	Import(context.Context, Snapshot) (ImportResult, error)
	DeleteRange(context.Context, RangeOptions) (DeleteRangeResult, error)
}

type SetRequest struct {
	Key     Key
	Value   []byte
	Options SetOptions
}

type SetResult struct {
	Record Record
	Found  bool
}

type GetRequest struct {
	Key     Key
	Options GetOptions
}

type TouchRequest struct {
	Key     Key
	Options TouchOptions
}

type GetResult struct {
	Record Record
	Found  bool
}

type DeleteResult struct {
	Deleted bool
	Found   bool
}

type ExistsResult struct {
	Exists bool
}

type TouchResult struct {
	Touched bool
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

func (s *EngineService) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (SetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.admitSet(ctx, key, value, opts); err != nil {
		return SetResult{}, err
	}

	record, applied, err := s.engine.Set(ctx, key, value, engine.SetOptions{
		TTL:              opts.TTL,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return SetResult{}, fmt.Errorf("set engine record: %w", err)
	}
	return SetResult{Record: record, Found: applied}, nil
}

func (s *EngineService) Get(ctx context.Context, key Key, opts GetOptions) (Record, bool, error) {
	record, found, err := s.engine.Get(ctx, key, engine.GetOptions{
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
	if err != nil {
		return Record{}, false, fmt.Errorf("get engine record: %w", err)
	}
	return record, found, nil
}

func (s *EngineService) Delete(ctx context.Context, key Key, options DeleteOptions) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted, applied, err := s.engine.Delete(ctx, key, engine.DeleteOptions{
		ExpectedVersion: options.ExpectedVersion,
	})
	if err != nil {
		return false, false, fmt.Errorf("delete engine record: %w", err)
	}
	return deleted, applied, nil
}

func (s *EngineService) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	found, err := s.engine.Exists(ctx, key, engine.GetOptions{
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
	if err != nil {
		return false, fmt.Errorf("check engine record: %w", err)
	}
	return found, nil
}

func (s *EngineService) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	touched, err := s.engine.Touch(ctx, key, engine.TouchOptions{
		TTL:              opts.TTL,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return false, fmt.Errorf("touch engine record: %w", err)
	}
	return touched, nil
}

func (s *EngineService) Adjust(ctx context.Context, key Key, opts AdjustOptions) (SetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.admitAdjust(ctx, key, opts); err != nil {
		return SetResult{}, err
	}

	record, applied, err := s.engine.Adjust(ctx, key, engine.AdjustOptions{
		Delta:            opts.Delta,
		InitialValue:     opts.InitialValue,
		TTL:              opts.TTL,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return SetResult{}, fmt.Errorf("adjust engine record: %w", err)
	}
	return SetResult{Record: record, Found: applied}, nil
}

func (s *EngineService) Primitive(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	if request.Kind.Mutates() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.admitPrimitive(ctx, request); err != nil {
			return PrimitiveResult{}, err
		}
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

func (s *EngineService) Stats(ctx context.Context) (Stats, error) {
	stats, err := s.engine.Stats(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("read engine stats: %w", err)
	}
	return stats, nil
}

func (s *EngineService) Evict(ctx context.Context, request EvictRequest) (EvictResult, error) {
	result, err := s.engine.Evict(ctx, engine.EvictOptions{
		Namespace:     request.Namespace,
		Space:         request.Space,
		TargetBytes:   request.TargetBytes,
		Exclude:       request.Exclude,
		ExcludeActive: request.Exclude.Key != "",
	})
	if err != nil {
		return EvictResult{}, fmt.Errorf("evict engine records: %w", err)
	}
	return result, nil
}
