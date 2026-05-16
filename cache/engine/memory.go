package engine

import (
	"context"
	"hash/fnv"
	"sort"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/samber/oops"
)

func NewMemory(cfg Config) *MemoryEngine {
	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 16
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	eng := &MemoryEngine{
		done:   make(chan struct{}),
		shards: make([]*shardWorker, shardCount),
		now:    now,
	}

	for i := range eng.shards {
		worker := &shardWorker{
			id:       i,
			commands: make(chan shardCommand, queueSize),
			entries:  collectionmapping.NewMap[string, *entry](),
			spaces:   collectionmapping.NewMap[spaceKey, spaceUsage](),
		}
		eng.shards[i] = worker
		eng.wg.Go(func() {
			worker.run(eng.done)
		})
	}

	return eng
}

func (e *MemoryEngine) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (Record, bool, error) {
	if err := validateKey(key); err != nil {
		return Record{}, false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandSet,
		physical: physicalKey(key),
		key:      key,
		value:    append([]byte(nil), value...),
		setOpts:  opts,
		now:      e.now(),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return Record{}, false, err
	}
	return result.record, result.found, result.err
}

func (e *MemoryEngine) Get(ctx context.Context, key Key, opts GetOptions) (Record, bool, error) {
	if err := validateKey(key); err != nil {
		return Record{}, false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandGet,
		physical: physicalKey(key),
		getOpts:  opts,
		now:      e.now(),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return Record{}, false, err
	}
	return result.record, result.found, result.err
}

func (e *MemoryEngine) Delete(ctx context.Context, key Key, opts DeleteOptions) (bool, bool, error) {
	if err := validateKey(key); err != nil {
		return false, false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:       commandDelete,
		physical:   physicalKey(key),
		deleteOpts: opts,
		reply:      make(chan shardResult, 1),
	})
	if err != nil {
		return false, false, err
	}
	return result.deleted, result.found, result.err
}

func (e *MemoryEngine) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	_, ok, err := e.Get(ctx, key, opts)
	return ok, err
}

func (e *MemoryEngine) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandTouch,
		physical: physicalKey(key),
		touch:    opts,
		now:      e.now(),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return false, err
	}
	return result.touched, result.err
}

func (e *MemoryEngine) Adjust(ctx context.Context, key Key, opts AdjustOptions) (Record, bool, error) {
	if err := validateKey(key); err != nil {
		return Record{}, false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandAdjust,
		physical: physicalKey(key),
		key:      key,
		adjust:   opts,
		now:      e.now(),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return Record{}, false, err
	}
	return result.record, result.found, result.err
}

func (e *MemoryEngine) Stats(ctx context.Context) (Stats, error) {
	stats := Stats{Shards: make([]ShardStats, len(e.shards))}
	spaces := collectionmapping.NewMap[spaceKey, spaceUsage]()
	for i, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{kind: commandStats, reply: make(chan shardResult, 1)})
		if err != nil {
			return Stats{}, err
		}
		if result.err != nil {
			return Stats{}, result.err
		}
		addShardStats(&stats, result.stats)
		addSpaceStats(spaces, result.spaces)
		stats.Shards[i] = result.stats
	}
	stats.Spaces = buildSpaceStats(spaces)
	return stats, nil
}

func (e *MemoryEngine) SweepExpired(ctx context.Context, now time.Time) (uint64, error) {
	var deleted uint64
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{kind: commandSweep, now: now, reply: make(chan shardResult, 1)})
		if err != nil {
			return deleted, err
		}
		if result.err != nil {
			return deleted, result.err
		}
		deleted += result.swept
	}
	return deleted, nil
}

func (e *MemoryEngine) Evict(ctx context.Context, opts EvictOptions) (EvictResult, error) {
	if opts.TargetBytes == 0 {
		return EvictResult{}, nil
	}
	if opts.Namespace == "" || opts.Space == "" {
		return EvictResult{}, ErrInvalidKey
	}
	if opts.Now.IsZero() {
		opts.Now = e.now()
	}
	return e.evictFromShards(ctx, opts)
}

func (e *MemoryEngine) evictFromShards(ctx context.Context, opts EvictOptions) (EvictResult, error) {
	total := EvictResult{RequestedBytes: opts.TargetBytes}
	for _, worker := range e.shards {
		if total.FreedBytes >= opts.TargetBytes {
			return total, nil
		}
		shardOpts := opts
		shardOpts.TargetBytes = opts.TargetBytes - total.FreedBytes
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:  commandEvict,
			evict: shardOpts,
			now:   opts.Now,
			reply: make(chan shardResult, 1),
		})
		if err != nil {
			return total, err
		}
		if result.err != nil {
			return total, result.err
		}
		total.FreedBytes += result.evicted.FreedBytes
		total.EvictedObjects += result.evicted.EvictedObjects
	}
	return total, nil
}

func (e *MemoryEngine) Close() error {
	e.once.Do(func() {
		close(e.done)
		e.wg.Wait()
	})
	return nil
}

func (e *MemoryEngine) execute(ctx context.Context, cmd shardCommand) (shardResult, error) {
	return e.executeOn(ctx, e.shardFor(cmd.physical), cmd)
}

func (e *MemoryEngine) executeOn(ctx context.Context, worker *shardWorker, cmd shardCommand) (shardResult, error) {
	if ctx == nil {
		return shardResult{}, ErrNilContext
	}

	select {
	case <-e.done:
		return shardResult{}, ErrClosed
	default:
	}

	select {
	case worker.commands <- cmd:
	case <-ctx.Done():
		return shardResult{}, oops.Code("shard_command_send_failed").
			In("cache.engine").
			Wrap(ctx.Err())
	case <-e.done:
		return shardResult{}, ErrClosed
	}

	select {
	case result := <-cmd.reply:
		return result, nil
	case <-ctx.Done():
		return shardResult{}, oops.Code("shard_command_wait_failed").
			In("cache.engine").
			Wrap(ctx.Err())
	case <-e.done:
		return shardResult{}, ErrClosed
	}
}

func (e *MemoryEngine) shardFor(physical string) *shardWorker {
	hash := fnv.New64a()
	if _, err := hash.Write([]byte(physical)); err != nil {
		return e.shards[0]
	}
	idx64 := hash.Sum64() % uint64(len(e.shards))
	for idx := range e.shards {
		if uint64(idx) == idx64 {
			return e.shards[idx]
		}
	}
	return e.shards[0]
}

func addShardStats(stats *Stats, shard ShardStats) {
	stats.Objects += shard.Objects
	stats.MemoryBytes += shard.MemoryBytes
	stats.Evictions += shard.Evictions
	stats.GetRequests += shard.GetRequests
	stats.GetHits += shard.GetHits
	stats.GetMisses += shard.GetMisses
	stats.GetExpired += shard.GetExpired
	stats.TouchRequests += shard.TouchRequests
	stats.TouchHits += shard.TouchHits
	stats.TouchMisses += shard.TouchMisses
}

func addSpaceStats(total, shard *collectionmapping.Map[spaceKey, spaceUsage]) {
	shard.Range(func(key spaceKey, usage spaceUsage) bool {
		next, _ := total.Get(key)
		next.objects += usage.objects
		next.memoryBytes += usage.memoryBytes
		total.Set(key, next)
		return true
	})
}

func buildSpaceStats(spaces *collectionmapping.Map[spaceKey, spaceUsage]) []SpaceStats {
	out := make([]SpaceStats, 0, spaces.Len())
	spaces.Range(func(key spaceKey, usage spaceUsage) bool {
		out = append(out, SpaceStats{
			Namespace:   key.namespace,
			Space:       key.space,
			Objects:     usage.objects,
			MemoryBytes: usage.memoryBytes,
		})
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].Space < out[j].Space
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}
