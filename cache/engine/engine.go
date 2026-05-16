// Package engine provides the in-memory node storage engine.
package engine

import (
	"context"
	"sync"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/samber/oops"
)

var (
	ErrInvalidKey       = oops.Code("invalid_key").In("cache.engine").New("engine: invalid key")
	ErrNotFound         = oops.Code("not_found").In("cache.engine").New("engine: not found")
	ErrClosed           = oops.Code("closed").In("cache.engine").New("engine: closed")
	ErrNilContext       = oops.Code("nil_context").In("cache.engine").New("engine: nil context")
	ErrInvalidCounter   = oops.Code("invalid_counter").In("cache.engine").New("engine: invalid counter value")
	ErrInvalidPrimitive = oops.Code("invalid_primitive").
				In("cache.engine").
				New("engine: invalid primitive request")
	ErrInvalidCollection = oops.Code("invalid_collection").
				In("cache.engine").
				New("engine: invalid collection value")
)

type Config struct {
	ShardCount    int
	SweepInterval time.Duration
	QueueSize     int
	Now           func() time.Time
}

type Key struct {
	Namespace string
	Space     string
	Entity    string
	Key       string
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

type EvictOptions struct {
	Namespace     string
	Space         string
	TargetBytes   uint64
	Exclude       Key
	ExcludeActive bool
	Now           time.Time
}

type EvictResult struct {
	RequestedBytes uint64
	FreedBytes     uint64
	EvictedObjects uint64
}

type Record struct {
	Key              Key
	Value            []byte
	CostBytes        uint64
	Version          uint64
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpireAt         time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	LastAccessAt     time.Time
	AccessCount      uint64
}

type Stats struct {
	Objects       uint64
	MemoryBytes   uint64
	Evictions     uint64
	GetRequests   uint64 `json:"get_requests"`
	GetHits       uint64 `json:"get_hits"`
	GetMisses     uint64 `json:"get_misses"`
	GetExpired    uint64 `json:"get_expired"`
	TouchRequests uint64 `json:"touch_requests"`
	TouchHits     uint64 `json:"touch_hits"`
	TouchMisses   uint64 `json:"touch_misses"`
	Shards        []ShardStats
	Spaces        []SpaceStats
}

type ShardStats struct {
	ID            int    `json:"id"`
	Objects       uint64 `json:"objects"`
	MemoryBytes   uint64 `json:"memory_bytes"`
	Evictions     uint64 `json:"evictions"`
	GetRequests   uint64 `json:"get_requests"`
	GetHits       uint64 `json:"get_hits"`
	GetMisses     uint64 `json:"get_misses"`
	GetExpired    uint64 `json:"get_expired"`
	TouchRequests uint64 `json:"touch_requests"`
	TouchHits     uint64 `json:"touch_hits"`
	TouchMisses   uint64 `json:"touch_misses"`
	QueueDepth    int    `json:"queue_depth"`
}

type SpaceStats struct {
	Namespace   string `json:"namespace"`
	Space       string `json:"space"`
	Objects     uint64 `json:"objects"`
	MemoryBytes uint64 `json:"memory_bytes"`
}

type DeleteOptions struct {
	ExpectedVersion uint64
}

type Engine interface {
	Set(context.Context, Key, []byte, SetOptions) (Record, bool, error)
	Get(context.Context, Key, GetOptions) (Record, bool, error)
	Delete(context.Context, Key, DeleteOptions) (bool, bool, error)
	Exists(context.Context, Key, GetOptions) (bool, error)
	Touch(context.Context, Key, TouchOptions) (bool, error)
	Adjust(context.Context, Key, AdjustOptions) (Record, bool, error)
	Primitive(context.Context, PrimitiveRequest) (PrimitiveResult, error)
	Stats(context.Context) (Stats, error)
	SweepExpired(context.Context, time.Time) (uint64, error)
	Evict(context.Context, EvictOptions) (EvictResult, error)
	Close() error
}

type MemoryEngine struct {
	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	shards []*shardWorker
	now    func() time.Time
}

type shardWorker struct {
	id       int
	commands chan shardCommand

	entries     *collectionmapping.Map[string, *entry]
	spaces      *collectionmapping.Map[spaceKey, spaceUsage]
	objects     uint64
	memoryBytes uint64
	evictions   uint64
	gets        uint64
	getHits     uint64
	getMisses   uint64
	getExpired  uint64
	touches     uint64
	touchHits   uint64
	touchMisses uint64
}

type spaceKey struct {
	namespace string
	space     string
}

type spaceUsage struct {
	objects     uint64
	memoryBytes uint64
}

type commandKind uint8

const (
	commandSet commandKind = iota + 1
	commandGet
	commandDelete
	commandTouch
	commandAdjust
	commandPrimitive
	commandStats
	commandSweep
	commandEvict
)

type shardCommand struct {
	kind       commandKind
	physical   string
	key        Key
	value      []byte
	setOpts    SetOptions
	getOpts    GetOptions
	touch      TouchOptions
	adjust     AdjustOptions
	primitive  PrimitiveRequest
	deleteOpts DeleteOptions
	evict      EvictOptions
	now        time.Time
	reply      chan shardResult
}

type shardResult struct {
	record    Record
	found     bool
	deleted   bool
	touched   bool
	primitive PrimitiveResult
	stats     ShardStats
	spaces    *collectionmapping.Map[spaceKey, spaceUsage]
	swept     uint64
	evicted   EvictResult
	err       error
}

type entry struct {
	key              Key
	value            []byte
	version          uint64
	namespaceVersion uint64
	spaceVersion     uint64
	expireAt         time.Time
	createdAt        time.Time
	updatedAt        time.Time
	lastAccessAt     time.Time
	accessCount      uint64
	costBytes        uint64
}
