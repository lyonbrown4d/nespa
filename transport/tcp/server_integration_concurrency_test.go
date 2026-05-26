package tcp_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func BenchmarkServerParallelGet(b *testing.B) {
	for _, concurrency := range []int{8, 16, 32, 64} {
		b.Run(fmt.Sprintf("clients=%d", concurrency), func(sb *testing.B) {
			benchmarkServerParallelGet(sb, concurrency)
		})
	}
}

func BenchmarkServerParallelSetGet(b *testing.B) {
	for _, concurrency := range []int{8, 16, 32, 64} {
		b.Run(fmt.Sprintf("clients=%d", concurrency), func(sb *testing.B) {
			benchmarkServerParallelSetGet(sb, concurrency)
		})
	}
}

func BenchmarkServerParallelBatchSet(b *testing.B) {
	for _, concurrency := range []int{8, 16, 32, 64} {
		b.Run(fmt.Sprintf("clients=%d", concurrency), func(sb *testing.B) {
			benchmarkServerParallelBatchSet(sb, concurrency)
		})
	}
}

func benchmarkServerParallelGet(b *testing.B, concurrency int) {
	b.Helper()

	addr, stopServer := startServerForConcurrencyBenchmark(b)
	b.Cleanup(stopServer)

	seedParallelGetKeys(b, addr, concurrency)

	b.ReportAllocs()
	b.ResetTimer()

	failures := runParallelBenchmark(b, addr, concurrency, parallelGetWorker(addr))
	requireNoParallelBenchmarkFailures(b, "parallel get", failures)
}

func benchmarkServerParallelSetGet(b *testing.B, concurrency int) {
	b.Helper()

	addr, stopServer := startServerForConcurrencyBenchmark(b)
	b.Cleanup(stopServer)

	b.ReportAllocs()
	b.ResetTimer()

	failures := runParallelBenchmark(b, addr, concurrency, parallelSetGetWorker(addr))
	requireNoParallelBenchmarkFailures(b, "parallel set/get", failures)
}

func benchmarkServerParallelBatchSet(b *testing.B, concurrency int) {
	b.Helper()

	addr, stopServer := startServerForConcurrencyBenchmark(b)
	b.Cleanup(stopServer)

	b.ReportAllocs()
	b.ResetTimer()

	failures := runParallelBenchmark(b, addr, concurrency, parallelBatchSetWorker(addr))
	requireNoParallelBenchmarkFailures(b, "parallel batch set", failures)
}

type parallelBenchmarkWorker func(*benchmarkFrameClient) error

type parallelBenchmarkWorkerFactory func(int) parallelBenchmarkWorker

func seedParallelGetKeys(b *testing.B, addr string, concurrency int) {
	b.Helper()

	seedClient, err := newBenchmarkFrameClientForConcurrency(addr)
	if err != nil {
		b.Fatalf("seed benchmark frame client: %v", err)
	}
	defer closeBenchmarkFrameClient(b, seedClient)

	for worker := range concurrency {
		if _, err := seedClient.Set(context.Background(), addr, cachewire.SetRequest{
			Key:       benchmarkBatchKey(fmt.Sprintf("parallel-get-%d", worker), 0),
			Value:     []byte("value"),
			TTLMillis: 60_000,
		}); err != nil {
			b.Fatalf("seed set key parallel-get-%d: %v", worker, err)
		}
	}
}

func runParallelBenchmark(
	b *testing.B,
	addr string,
	concurrency int,
	factory parallelBenchmarkWorkerFactory,
) uint64 {
	b.Helper()

	limit := benchmarkLimit(b)
	var ops atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for worker := range concurrency {
		client := newParallelBenchmarkClient(b, addr)
		go runParallelBenchmarkWorker(b, &wg, &ops, &failures, limit, client, factory(worker))
	}

	wg.Wait()
	return failures.Load()
}

func newParallelBenchmarkClient(b *testing.B, addr string) *benchmarkFrameClient {
	b.Helper()

	client, err := newBenchmarkFrameClientForConcurrency(addr)
	if err != nil {
		b.Fatalf("create benchmark frame client: %v", err)
	}
	return client
}

func runParallelBenchmarkWorker(
	b *testing.B,
	wg *sync.WaitGroup,
	ops *atomic.Uint64,
	failures *atomic.Uint64,
	limit uint64,
	client *benchmarkFrameClient,
	worker parallelBenchmarkWorker,
) {
	b.Helper()

	defer wg.Done()
	defer closeBenchmarkFrameClient(b, client)

	for claimParallelBenchmarkOp(ops, limit) {
		if err := worker(client); err != nil {
			failures.Add(1)
			return
		}
	}
}

func claimParallelBenchmarkOp(ops *atomic.Uint64, limit uint64) bool {
	return ops.Add(1) <= limit
}

func parallelGetWorker(addr string) parallelBenchmarkWorkerFactory {
	return func(worker int) parallelBenchmarkWorker {
		key := benchmarkBatchKey(fmt.Sprintf("parallel-get-%d", worker), 0)
		return func(client *benchmarkFrameClient) error {
			_, err := client.Get(context.Background(), addr, cachewire.GetRequest{Key: key})
			return err
		}
	}
}

func parallelSetGetWorker(addr string) parallelBenchmarkWorkerFactory {
	return func(worker int) parallelBenchmarkWorker {
		key := benchmarkBatchKey(fmt.Sprintf("parallel-setget-%d", worker), 0)
		return func(client *benchmarkFrameClient) error {
			if _, err := client.Set(context.Background(), addr, cachewire.SetRequest{
				Key:       key,
				Value:     []byte("value"),
				TTLMillis: 60_000,
			}); err != nil {
				return err
			}
			_, err := client.Get(context.Background(), addr, cachewire.GetRequest{Key: key})
			return err
		}
	}
}

func parallelBatchSetWorker(addr string) parallelBenchmarkWorkerFactory {
	return func(worker int) parallelBenchmarkWorker {
		request := parallelBatchSetRequest(worker)
		return func(client *benchmarkFrameClient) error {
			_, err := client.BatchSet(context.Background(), addr, request)
			return err
		}
	}
}

func parallelBatchSetRequest(worker int) cachewire.BatchSetRequest {
	const batchSize = 8

	request := cachewire.BatchSetRequest{
		Items: make([]cachewire.SetRequest, 0, batchSize),
	}
	for i := range batchSize {
		request.Items = append(request.Items, cachewire.SetRequest{
			Key:       benchmarkBatchKey(fmt.Sprintf("parallel-batch-%d", worker), i),
			Value:     []byte("batch-value"),
			TTLMillis: 60_000,
		})
	}
	return request
}

func requireNoParallelBenchmarkFailures(b *testing.B, name string, failures uint64) {
	b.Helper()

	if failures > 0 {
		b.Fatalf("%s failed: %d errors", name, failures)
	}
}

func startServerForConcurrencyBenchmark(b *testing.B) (string, func()) {
	b.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	server := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr: "127.0.0.1:0",
	}, cache.NewService(eng))

	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		b.Fatalf("start tcp server: %v", err)
	}

	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			b.Fatalf("stop tcp server: %v", err)
		}
		if err := eng.Close(); err != nil {
			b.Fatalf("close cache engine: %v", err)
		}
	}

	return server.Addr(), stop
}
