package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/arcgolabs/dix"
)

var (
	ErrInvalidKey = errors.New("engine: invalid key")
	ErrNotFound   = errors.New("engine: not found")
	ErrClosed     = errors.New("engine: closed")
)

type Config struct {
	ShardCount    int
	SweepInterval time.Duration
	QueueSize     int
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
}

type GetOptions struct {
	NamespaceVersion uint64
	SpaceVersion     uint64
}

type TouchOptions struct {
	TTL time.Duration
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
	Objects     uint64
	MemoryBytes uint64
	Evictions   uint64
	Shards      []ShardStats
	Spaces      []SpaceStats
}

type ShardStats struct {
	ID          int    `json:"id"`
	Objects     uint64 `json:"objects"`
	MemoryBytes uint64 `json:"memory_bytes"`
	Evictions   uint64 `json:"evictions"`
	QueueDepth  int    `json:"queue_depth"`
}

type SpaceStats struct {
	Namespace   string `json:"namespace"`
	Space       string `json:"space"`
	Objects     uint64 `json:"objects"`
	MemoryBytes uint64 `json:"memory_bytes"`
}

type Engine interface {
	Set(context.Context, Key, []byte, SetOptions) (Record, error)
	Get(context.Context, Key, GetOptions) (Record, bool, error)
	Delete(context.Context, Key) (bool, error)
	Exists(context.Context, Key, GetOptions) (bool, error)
	Touch(context.Context, Key, TouchOptions) (bool, error)
	Stats(context.Context) (Stats, error)
	SweepExpired(context.Context, time.Time) (uint64, error)
	Evict(context.Context, EvictOptions) (EvictResult, error)
	Close() error
}

type MemoryEngine struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	shards []*shardWorker
	now    func() time.Time
}

type shardWorker struct {
	id       int
	commands chan shardCommand

	entries     map[string]*entry
	spaces      map[spaceKey]spaceUsage
	objects     uint64
	memoryBytes uint64
	evictions   uint64
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
	commandStats
	commandSweep
	commandEvict
)

type shardCommand struct {
	kind     commandKind
	physical string
	key      Key
	value    []byte
	setOpts  SetOptions
	getOpts  GetOptions
	touch    TouchOptions
	evict    EvictOptions
	now      time.Time
	reply    chan shardResult
}

type shardResult struct {
	record  Record
	found   bool
	deleted bool
	touched bool
	stats   ShardStats
	spaces  map[spaceKey]spaceUsage
	swept   uint64
	evicted EvictResult
	err     error
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

func NewMemory(cfg Config) *MemoryEngine {
	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 16
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}

	ctx, cancel := context.WithCancel(context.Background())
	eng := &MemoryEngine{
		ctx:    ctx,
		cancel: cancel,
		shards: make([]*shardWorker, shardCount),
		now:    time.Now,
	}

	for i := range eng.shards {
		worker := &shardWorker{
			id:       i,
			commands: make(chan shardCommand, queueSize),
			entries:  make(map[string]*entry),
			spaces:   make(map[spaceKey]spaceUsage),
		}
		eng.shards[i] = worker
		eng.wg.Add(1)
		go func() {
			defer eng.wg.Done()
			worker.run(ctx)
		}()
	}

	return eng
}

func Module(eng Engine, sweepInterval time.Duration) dix.Module {
	if sweepInterval <= 0 {
		sweepInterval = time.Second
	}

	return dix.NewModule("node.engine",
		dix.WithModuleProviders(
			dix.Value[Engine](eng),
		),
		dix.WithModuleHooks(
			dix.OnStart[Engine](func(ctx context.Context, eng Engine) error {
				go runSweeper(ctx, eng, sweepInterval)
				return nil
			}, dix.LifecycleName("node.engine.sweeper.start")),
			dix.OnStop[Engine](func(_ context.Context, eng Engine) error {
				return eng.Close()
			}, dix.LifecycleName("node.engine.stop")),
		),
	)
}

func runSweeper(ctx context.Context, eng Engine, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, _ = eng.SweepExpired(ctx, now)
		}
	}
}

func (e *MemoryEngine) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (Record, error) {
	if err := validateKey(key); err != nil {
		return Record{}, err
	}

	now := e.now()
	cmd := shardCommand{
		kind:     commandSet,
		physical: physicalKey(key),
		key:      key,
		value:    append([]byte(nil), value...),
		setOpts:  opts,
		now:      now,
		reply:    make(chan shardResult, 1),
	}

	result, err := e.execute(ctx, cmd)
	if err != nil {
		return Record{}, err
	}
	return result.record, result.err
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

func (e *MemoryEngine) Delete(ctx context.Context, key Key) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandDelete,
		physical: physicalKey(key),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return false, err
	}
	return result.deleted, result.err
}

func (e *MemoryEngine) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	_, ok, err := e.Get(ctx, key, opts)
	return ok, err
}

func (e *MemoryEngine) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if opts.TTL <= 0 {
		return false, fmt.Errorf("engine: touch ttl must be positive")
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

func (e *MemoryEngine) Stats(ctx context.Context) (Stats, error) {
	stats := Stats{Shards: make([]ShardStats, len(e.shards))}
	spaces := make(map[spaceKey]spaceUsage)
	for i, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:  commandStats,
			reply: make(chan shardResult, 1),
		})
		if err != nil {
			return Stats{}, err
		}
		if result.err != nil {
			return Stats{}, result.err
		}
		stats.Shards[i] = result.stats
		stats.Objects += result.stats.Objects
		stats.MemoryBytes += result.stats.MemoryBytes
		stats.Evictions += result.stats.Evictions
		for key, usage := range result.spaces {
			total := spaces[key]
			total.objects += usage.objects
			total.memoryBytes += usage.memoryBytes
			spaces[key] = total
		}
	}
	stats.Spaces = make([]SpaceStats, 0, len(spaces))
	for key, usage := range spaces {
		stats.Spaces = append(stats.Spaces, SpaceStats{
			Namespace:   key.namespace,
			Space:       key.space,
			Objects:     usage.objects,
			MemoryBytes: usage.memoryBytes,
		})
	}
	sort.Slice(stats.Spaces, func(i, j int) bool {
		if stats.Spaces[i].Namespace == stats.Spaces[j].Namespace {
			return stats.Spaces[i].Space < stats.Spaces[j].Space
		}
		return stats.Spaces[i].Namespace < stats.Spaces[j].Namespace
	})
	return stats, nil
}

func (e *MemoryEngine) SweepExpired(ctx context.Context, now time.Time) (uint64, error) {
	var deleted uint64
	for _, worker := range e.shards {
		result, err := e.executeOn(ctx, worker, shardCommand{
			kind:  commandSweep,
			now:   now,
			reply: make(chan shardResult, 1),
		})
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

	total := EvictResult{RequestedBytes: opts.TargetBytes}
	for _, worker := range e.shards {
		remaining := opts.TargetBytes - total.FreedBytes
		if remaining == 0 {
			break
		}
		shardOpts := opts
		shardOpts.TargetBytes = remaining
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
		e.cancel()
		e.wg.Wait()
	})
	return nil
}

func (e *MemoryEngine) execute(ctx context.Context, cmd shardCommand) (shardResult, error) {
	return e.executeOn(ctx, e.shardFor(cmd.physical), cmd)
}

func (e *MemoryEngine) executeOn(ctx context.Context, worker *shardWorker, cmd shardCommand) (shardResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-e.ctx.Done():
		return shardResult{}, ErrClosed
	default:
	}

	select {
	case worker.commands <- cmd:
	case <-ctx.Done():
		return shardResult{}, ctx.Err()
	case <-e.ctx.Done():
		return shardResult{}, ErrClosed
	}

	select {
	case result := <-cmd.reply:
		return result, nil
	case <-ctx.Done():
		return shardResult{}, ctx.Err()
	case <-e.ctx.Done():
		return shardResult{}, ErrClosed
	}
}

func (e *MemoryEngine) shardFor(physical string) *shardWorker {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(physical))
	return e.shards[int(hash.Sum64()%uint64(len(e.shards)))]
}

func (s *shardWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-s.commands:
			cmd.reply <- s.apply(cmd)
		}
	}
}

func (s *shardWorker) apply(cmd shardCommand) shardResult {
	switch cmd.kind {
	case commandSet:
		return s.applySet(cmd)
	case commandGet:
		return s.applyGet(cmd)
	case commandDelete:
		return s.applyDelete(cmd)
	case commandTouch:
		return s.applyTouch(cmd)
	case commandStats:
		spaces := make(map[spaceKey]spaceUsage, len(s.spaces))
		for key, usage := range s.spaces {
			spaces[key] = usage
		}
		return shardResult{stats: ShardStats{
			ID:          s.id,
			Objects:     s.objects,
			MemoryBytes: s.memoryBytes,
			Evictions:   s.evictions,
			QueueDepth:  len(s.commands),
		}, spaces: spaces}
	case commandSweep:
		return shardResult{swept: s.sweepExpired(cmd.now)}
	case commandEvict:
		return shardResult{evicted: s.evict(cmd.evict)}
	default:
		return shardResult{err: fmt.Errorf("engine: unknown shard command %d", cmd.kind)}
	}
}

func (s *shardWorker) applySet(cmd shardCommand) shardResult {
	var expireAt time.Time
	if cmd.setOpts.TTL > 0 {
		expireAt = cmd.now.Add(cmd.setOpts.TTL)
	}

	cost := costOf(cmd.key, cmd.value)
	existing, ok := s.entries[cmd.physical]
	if ok {
		if cost >= existing.costBytes {
			delta := cost - existing.costBytes
			s.memoryBytes += delta
			s.addSpaceUsage(spaceKeyOf(existing.key), 0, delta)
		} else {
			delta := existing.costBytes - cost
			s.memoryBytes -= delta
			s.subtractSpaceUsage(spaceKeyOf(existing.key), 0, delta)
		}
		existing.value = cmd.value
		existing.version++
		existing.namespaceVersion = cmd.setOpts.NamespaceVersion
		existing.spaceVersion = cmd.setOpts.SpaceVersion
		existing.expireAt = expireAt
		existing.updatedAt = cmd.now
		existing.lastAccessAt = cmd.now
		existing.accessCount++
		existing.costBytes = cost
		return shardResult{record: existing.record()}
	}

	ent := &entry{
		key:              cmd.key,
		value:            cmd.value,
		version:          1,
		namespaceVersion: cmd.setOpts.NamespaceVersion,
		spaceVersion:     cmd.setOpts.SpaceVersion,
		expireAt:         expireAt,
		createdAt:        cmd.now,
		updatedAt:        cmd.now,
		lastAccessAt:     cmd.now,
		accessCount:      1,
		costBytes:        cost,
	}
	s.entries[cmd.physical] = ent
	s.objects++
	s.memoryBytes += cost
	s.addSpaceUsage(spaceKeyOf(cmd.key), 1, cost)

	return shardResult{record: ent.record()}
}

func (s *shardWorker) applyGet(cmd shardCommand) shardResult {
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return shardResult{}
	}
	if !ent.visible(cmd.getOpts) {
		return shardResult{}
	}
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	return shardResult{record: ent.record(), found: true}
}

func (s *shardWorker) applyDelete(cmd shardCommand) shardResult {
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	s.deleteEntry(cmd.physical, ent)
	return shardResult{deleted: true}
}

func (s *shardWorker) applyTouch(cmd shardCommand) shardResult {
	ent, ok := s.entries[cmd.physical]
	if !ok {
		return shardResult{}
	}
	if ent.expired(cmd.now) {
		s.deleteEntry(cmd.physical, ent)
		return shardResult{}
	}

	ent.expireAt = cmd.now.Add(cmd.touch.TTL)
	ent.updatedAt = cmd.now
	ent.lastAccessAt = cmd.now
	ent.accessCount++
	return shardResult{touched: true}
}

func (s *shardWorker) evict(opts EvictOptions) EvictResult {
	result := EvictResult{RequestedBytes: opts.TargetBytes}
	candidates := make([]*entry, 0)
	excludePhysical := ""
	if opts.ExcludeActive {
		excludePhysical = physicalKey(opts.Exclude)
	}

	for physical, ent := range s.entries {
		if excludePhysical != "" && physical == excludePhysical {
			continue
		}
		if ent.key.Namespace != opts.Namespace || ent.key.Space != opts.Space {
			continue
		}
		if ent.expired(opts.Now) {
			result.FreedBytes += ent.costBytes
			result.EvictedObjects++
			s.deleteEntry(physical, ent)
			continue
		}
		candidates = append(candidates, ent)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lastAccessAt.Equal(candidates[j].lastAccessAt) {
			return candidates[i].createdAt.Before(candidates[j].createdAt)
		}
		return candidates[i].lastAccessAt.Before(candidates[j].lastAccessAt)
	})

	for _, ent := range candidates {
		if result.FreedBytes >= opts.TargetBytes {
			break
		}
		result.FreedBytes += ent.costBytes
		result.EvictedObjects++
		s.deleteEntry(physicalKey(ent.key), ent)
	}

	if result.EvictedObjects > 0 {
		s.evictions += result.EvictedObjects
	}
	return result
}

func (s *shardWorker) sweepExpired(now time.Time) uint64 {
	var deleted uint64
	for physical, ent := range s.entries {
		if ent.expired(now) {
			s.deleteEntry(physical, ent)
			deleted++
		}
	}
	return deleted
}

func (s *shardWorker) deleteEntry(physical string, ent *entry) {
	delete(s.entries, physical)
	s.objects--
	s.memoryBytes -= ent.costBytes
	s.subtractSpaceUsage(spaceKeyOf(ent.key), 1, ent.costBytes)
}

func (s *shardWorker) addSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage := s.spaces[key]
	usage.objects += objects
	usage.memoryBytes += memoryBytes
	s.spaces[key] = usage
}

func (s *shardWorker) subtractSpaceUsage(key spaceKey, objects, memoryBytes uint64) {
	usage := s.spaces[key]
	usage.objects -= objects
	usage.memoryBytes -= memoryBytes
	if usage.objects == 0 && usage.memoryBytes == 0 {
		delete(s.spaces, key)
		return
	}
	s.spaces[key] = usage
}

func (e *entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !e.expireAt.After(now)
}

func (e *entry) visible(opts GetOptions) bool {
	if opts.NamespaceVersion != 0 && e.namespaceVersion != opts.NamespaceVersion {
		return false
	}
	if opts.SpaceVersion != 0 && e.spaceVersion != opts.SpaceVersion {
		return false
	}
	return true
}

func (e *entry) record() Record {
	return Record{
		Key:              e.key,
		Value:            append([]byte(nil), e.value...),
		CostBytes:        e.costBytes,
		Version:          e.version,
		NamespaceVersion: e.namespaceVersion,
		SpaceVersion:     e.spaceVersion,
		ExpireAt:         e.expireAt,
		CreatedAt:        e.createdAt,
		UpdatedAt:        e.updatedAt,
		LastAccessAt:     e.lastAccessAt,
		AccessCount:      e.accessCount,
	}
}

func validateKey(key Key) error {
	if key.Namespace == "" || key.Space == "" || key.Key == "" {
		return ErrInvalidKey
	}
	return nil
}

func physicalKey(key Key) string {
	return key.Namespace + "\x00" + key.Space + "\x00" + key.Entity + "\x00" + key.Key
}

func spaceKeyOf(key Key) spaceKey {
	return spaceKey{namespace: key.Namespace, space: key.Space}
}

func EstimateCost(key Key, value []byte) uint64 {
	return costOf(key, value)
}

func costOf(key Key, value []byte) uint64 {
	return uint64(len(key.Namespace) + len(key.Space) + len(key.Entity) + len(key.Key) + len(value))
}
