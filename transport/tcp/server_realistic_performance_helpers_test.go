package tcp_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
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

type profileOperation func(context.Context, *benchmarkFrameClient, uint64) error

func runProfileScenario(
	t *testing.T,
	addr string,
	operations uint64,
	op profileOperation,
	name string,
	payloadSize int,
) profileResult {
	t.Helper()

	if operations == 0 {
		t.Fatalf("operations = %d, want > 0", operations)
	}

	clients := openProfileClients(t, addr)
	defer closeProfileClients(t, clients)

	samples, failCount, elapsed := runProfileWorkers(operations, clients, op)
	slices.Sort(samples)

	result := profileResult{
		name:         name,
		p50:          percentileDuration(samples, 0.50),
		p95:          percentileDuration(samples, 0.95),
		p99:          percentileDuration(samples, 0.99),
		qps:          float64(len(samples)) / elapsed.Seconds(),
		failureRate:  float64(failCount) / float64(operations),
		payloadBytes: payloadSize,
		operations:   operations,
		concurrency:  profileConcurrency,
		elapsed:      elapsed,
	}

	t.Logf("profile result name=%s ops=%d concurrency=%d payload=%dB fail=%d failRate=%d%% elapsed=%s p50=%s p95=%s p99=%s qps=%.2f",
		name,
		operations,
		profileConcurrency,
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

func openProfileClients(t *testing.T, addr string) []*benchmarkFrameClient {
	t.Helper()

	clients := make([]*benchmarkFrameClient, 0, profileConcurrency)
	for index := range profileConcurrency {
		client, err := newBenchmarkFrameClientForConcurrency(addr)
		if err != nil {
			t.Fatalf("create benchmark frame client %d: %v", index, err)
		}
		clients = append(clients, client)
	}
	return clients
}

func closeProfileClients(t *testing.T, clients []*benchmarkFrameClient) {
	t.Helper()

	for _, client := range clients {
		if err := client.Close(); err != nil {
			t.Logf("close benchmark frame client: %v", err)
		}
	}
}

func runProfileWorkers(
	operations uint64,
	clients []*benchmarkFrameClient,
	op profileOperation,
) ([]time.Duration, uint64, time.Duration) {
	var samplesMu sync.Mutex
	samples := make([]time.Duration, 0)
	var failures atomic.Uint64
	var executed atomic.Uint64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(profileConcurrency)
	for worker := range profileConcurrency {
		go runProfileWorker(&wg, clients[worker], operations, &executed, &failures, &samplesMu, &samples, op)
	}
	wg.Wait()

	return samples, failures.Load(), time.Since(start)
}

func runProfileWorker(
	wg *sync.WaitGroup,
	client *benchmarkFrameClient,
	operations uint64,
	executed *atomic.Uint64,
	failures *atomic.Uint64,
	samplesMu *sync.Mutex,
	samples *[]time.Duration,
	op profileOperation,
) {
	defer wg.Done()

	for {
		opIndex, ok := nextProfileOperation(executed, operations)
		if !ok {
			return
		}
		started := time.Now()
		if err := op(context.Background(), client, opIndex); err != nil {
			failures.Add(1)
		}
		recordProfileSample(samplesMu, samples, time.Since(started))
	}
}

func nextProfileOperation(executed *atomic.Uint64, operations uint64) (uint64, bool) {
	opIndex := executed.Add(1) - 1
	return opIndex, opIndex < operations
}

func recordProfileSample(samplesMu *sync.Mutex, samples *[]time.Duration, elapsed time.Duration) {
	samplesMu.Lock()
	defer samplesMu.Unlock()

	*samples = append(*samples, elapsed)
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
	index = max(index, 0)
	index = min(index, len(samples)-1)
	return samples[index]
}

type profileResult struct {
	name         string
	payloadBytes int
	operations   uint64
	concurrency  int
	p50          time.Duration
	p95          time.Duration
	p99          time.Duration
	qps          float64
	failureRate  float64
	elapsed      time.Duration
}
