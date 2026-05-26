package tcp_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestProfileEndpointSetGetTraffic(t *testing.T) {
	addr, stopServer := resolveProfileTarget(t)
	defer stopServer()

	const operations = 20_000
	const concurrency = 16
	payloadSizes := []int{64, 1024, 16 * 1024}

	for _, payloadSize := range payloadSizes {
		t.Run(fmt.Sprintf("payload=%d", payloadSize), func(t *testing.T) {
			result := runProfileScenario(
				t,
				addr,
				concurrency,
				operations,
				func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
					key := benchmarkBatchKey("profile", int(opIndex))
					payload := bytes.Repeat([]byte("x"), payloadSize)
					if _, err := client.Set(ctx, addr, cachewire.SetRequest{
						Key:       key,
						Value:     payload,
						TTLMillis: 60_000,
					}); err != nil {
						return err
					}
					if _, err := client.Get(ctx, addr, cachewire.GetRequest{
						Key: key,
					}); err != nil {
						return err
					}
					return nil
				},
				"set+get",
				payloadSize,
			)
			t.Logf("endpoint set/get payload=%dB: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
				payloadSize, result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
		})
	}
}

func TestProfileReplicationThroughput(t *testing.T) {
	replica, _, stopReplica := startProfileServerWithService(t, cachetcp.ServerConfig{})
	defer stopReplica()

	primary, _, stopPrimary := startProfileServerWithService(t, cachetcp.ServerConfig{
		ReplicaTargets: func(key cachewire.Key) []string {
			return []string{replica.Addr()}
		},
		ReplicationOutboxPath: filepath.Join(t.TempDir(), "replication.outbox"),
		ReplicationQueueSize:  20_000,
	})
	defer stopPrimary()

	const operations = 2_000
	const payloadSize = 1024

	result := runProfileScenario(
		t,
		primary.Addr(),
		16,
		operations,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			return runSet(ctx, client, primary.Addr(), benchmarkBatchKey("replication", int(opIndex)), payloadSize)
		},
		"set-only",
		payloadSize,
	)

	if err := waitReplicationCatchup(primary, uint64(operations), 20*time.Second); err != nil {
		t.Logf("replication catchup: %v", err)
	}

	t.Logf("replication set: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
	t.Logf("replication stats=%+v", primary.ReplicationStats())
}

func TestProfilePersistenceSnapshotRestore(t *testing.T) {
	server, service, stopServer := startProfileServerWithService(t, cachetcp.ServerConfig{})
	defer stopServer()

	const writes = 8_000
	const reads = 5_000
	const payloadSize = 1024

	seedAddr := server.Addr()

	seedResult := runProfileScenario(
		t,
		seedAddr,
		16,
		writes,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			return runSet(ctx, client, seedAddr, benchmarkBatchKey("persist", int(opIndex)), payloadSize)
		},
		"snapshot seed set",
		payloadSize,
	)
	_ = seedResult

	exportStart := time.Now()
	snapshot, err := service.Export(context.Background(), cache.RangeOptions{
		Namespace:  "bench",
		Space:      "session",
		VSlotStart: 0,
		VSlotEnd:   ^uint32(0),
	})
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}
	exportDuration := time.Since(exportStart)

	restoreStart := time.Now()
	restoredServer, stopRestored := restoreProfileServerFromSnapshot(t, cachetcp.ServerConfig{}, snapshot)
	defer stopRestored()
	restoreDuration := time.Since(restoreStart)

	restoreResult := runProfileScenario(
		t,
		restoredServer.Addr(),
		16,
		reads,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			_, err := client.Get(ctx, restoredServer.Addr(), cachewire.GetRequest{
				Key: benchmarkBatchKey("persist", int(opIndex)),
			})
			return err
		},
		"snapshot restore read",
		payloadSize,
	)

	t.Logf("persistence seed set: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		seedResult.qps, seedResult.failureRate*100, seedResult.p50, seedResult.p95, seedResult.p99)
	t.Logf("persistence restore read: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		restoreResult.qps, restoreResult.failureRate*100, restoreResult.p50, restoreResult.p95, restoreResult.p99)
	t.Logf("persistence durations: snapshot=%s restore=%s", exportDuration, restoreDuration)
}

func TestProfileRoutingEpochMix(t *testing.T) {
	currentEpoch := atomic.Uint64{}
	currentEpoch.Store(2)

	server, _, stopServer := startProfileServerWithService(t, cachetcp.ServerConfig{
		CurrentRouteEpoch: currentEpoch.Load,
	})
	defer stopServer()

	const operations = 12_000
	const payloadSize = 256
	const concurrency = 16

	result := runProfileScenario(
		t,
		server.Addr(),
		concurrency,
		operations,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			key := benchmarkBatchKey("route", int(opIndex%4096))
			request := cachewire.SetRequest{
				Key:       key,
				Value:     bytes.Repeat([]byte("x"), payloadSize),
				TTLMillis: 60_000,
			}
			if opIndex%2 == 0 {
				request.RouteEpoch = 1 // stale for this setup
			} else {
				request.RouteEpoch = currentEpoch.Load()
			}
			_, err := client.Set(ctx, server.Addr(), request)
			return err
		},
		"route mixed epoch",
		payloadSize,
	)

	t.Logf("routing mixed epoch: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
}

func runProfileScenario(
	t *testing.T,
	addr string,
	concurrency int,
	operations int,
	op func(context.Context, *benchmarkFrameClient, uint64) error,
	name string,
	payloadSize int,
) profileResult {
	t.Helper()

	if operations <= 0 {
		t.Fatalf("operations = %d, want > 0", operations)
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	clients := make([]*benchmarkFrameClient, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		client, err := newBenchmarkFrameClientForConcurrency(addr)
		if err != nil {
			t.Fatalf("create benchmark frame client %d: %v", i, err)
		}
		clients = append(clients, client)
		defer func(c *benchmarkFrameClient) {
			if err := c.Close(); err != nil {
				t.Logf("close benchmark frame client: %v", err)
			}
		}(client)
	}

	var samplesMu sync.Mutex
	var samples []time.Duration
	var failures atomic.Uint64
	var executed atomic.Uint64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func(c *benchmarkFrameClient) {
			defer wg.Done()
			for {
				opIndex := executed.Add(1) - 1
				if int(opIndex) >= operations {
					return
				}
				started := time.Now()
				if err := op(context.Background(), c, opIndex); err != nil {
					failures.Add(1)
				}
				elapsed := time.Since(started)

				samplesMu.Lock()
				samples = append(samples, elapsed)
				samplesMu.Unlock()
			}
		}(clients[worker])
	}
	wg.Wait()

	elapsed := time.Since(start)

	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})
	failCount := failures.Load()

	result := profileResult{
		name:         name,
		p50:          percentileDuration(samples, 0.50),
		p95:          percentileDuration(samples, 0.95),
		p99:          percentileDuration(samples, 0.99),
		qps:          float64(len(samples)) / elapsed.Seconds(),
		failureRate:  float64(failCount) / float64(operations),
		payloadBytes: payloadSize,
		operations:   operations,
		concurrency:  concurrency,
		elapsed:      elapsed,
	}

	t.Logf("profile result name=%s ops=%d concurrency=%d payload=%dB fail=%d failRate=%d%% elapsed=%s p50=%s p95=%s p99=%s qps=%.2f",
		name,
		operations,
		concurrency,
		payloadSize,
		failCount,
		int(result.failureRate*100),
		elapsed,
		result.p50,
		result.p95,
		result.p99,
		result.qps,
	)

	return result
}

func runSet(
	ctx context.Context,
	client *benchmarkFrameClient,
	addr string,
	key cachewire.Key,
	payloadSize int,
) error {
	return runSetWithValue(ctx, client, addr, key, bytes.Repeat([]byte("x"), payloadSize))
}

func runSetWithValue(
	ctx context.Context,
	client *benchmarkFrameClient,
	addr string,
	key cachewire.Key,
	value []byte,
) error {
	_, err := client.Set(ctx, addr, cachewire.SetRequest{
		Key:       key,
		Value:     value,
		TTLMillis: 60_000,
	})
	return err
}

func resolveProfileTarget(t *testing.T) (string, func()) {
	t.Helper()

	addr := strings.TrimSpace(os.Getenv("NESPA_BENCH_ADDR"))
	if addr != "" {
		return addr, func() {}
	}

	server, _, stopServer := startProfileServerWithService(t, cachetcp.ServerConfig{Addr: "127.0.0.1:0"})
	return server.Addr(), stopServer
}

func startProfileServerWithService(
	t *testing.T,
	cfg cachetcp.ServerConfig,
) (*cachetcp.Server, *engine.MemoryEngine, func()) {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	service := cache.NewService(eng)
	server := cachetcp.NewServer(cfg, service)

	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start profile tcp server: %v", err)
	}
	stopServer := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Logf("stop profile tcp server: %v", err)
		}
		if err := eng.Close(); err != nil {
			t.Logf("close profile cache engine: %v", err)
		}
	}

	return server, eng, stopServer
}

func restoreProfileServerFromSnapshot(
	t *testing.T,
	cfg cachetcp.ServerConfig,
	snapshot cache.Snapshot,
) (*cachetcp.Server, func()) {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	service := cache.NewService(eng)

	if _, err := service.Import(context.Background(), snapshot); err != nil {
		t.Fatalf("import snapshot: %v", err)
	}

	server := cachetcp.NewServer(cfg, service)
	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start restored profile tcp server: %v", err)
	}

	stopServer := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Logf("stop restored profile tcp server: %v", err)
		}
		if err := eng.Close(); err != nil {
			t.Logf("close restored cache engine: %v", err)
		}
	}
	return server, stopServer
}

func waitReplicationCatchup(server *cachetcp.Server, want uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := server.ReplicationStats()
		if stats.Successes >= want && stats.LastSuccessSequence >= want {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	stats := server.ReplicationStats()
	return fmt.Errorf(
		"replication did not catch up: want >= %d successes, got %+v",
		want,
		stats,
	)
}

func percentileDuration(samples []time.Duration, ratio float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	index := int(math.Round((float64(len(samples)) - 1) * ratio))
	if index < 0 {
		index = 0
	}
	if index >= len(samples) {
		index = len(samples) - 1
	}
	return samples[index]
}

type profileResult struct {
	name         string
	payloadBytes int
	operations   int
	concurrency  int
	p50          time.Duration
	p95          time.Duration
	p99          time.Duration
	qps          float64
	failureRate  float64
	elapsed      time.Duration
}
