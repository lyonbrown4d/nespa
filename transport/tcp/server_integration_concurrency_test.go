package tcp_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
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

	seedClient, err := newBenchmarkFrameClientForConcurrency(addr)
	if err != nil {
		b.Fatalf("seed benchmark frame client: %v", err)
	}
	for worker := 0; worker < concurrency; worker++ {
		if _, err := seedClient.Set(context.Background(), addr, cachewire.SetRequest{
			Key:       benchmarkBatchKey(fmt.Sprintf("parallel-get-%d", worker), 0),
			Value:     []byte("value"),
			TTLMillis: 60_000,
		}); err != nil {
			if closeErr := seedClient.Close(); closeErr != nil {
				b.Fatalf("seed set before close: %v, close: %v", err, closeErr)
			}
			b.Fatalf("seed set key parallel-get-%d: %v", worker, err)
		}
	}
	if err := seedClient.Close(); err != nil {
		b.Fatalf("close seed benchmark frame client: %v", err)
	}

	var ops atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(concurrency)

	b.ReportAllocs()
	b.ResetTimer()

	for worker := 0; worker < concurrency; worker++ {
		client, err := newBenchmarkFrameClientForConcurrency(addr)
		if err != nil {
			b.Fatalf("create benchmark frame client: %v", err)
		}
		go func(w int, c *benchmarkFrameClient) {
			defer wg.Done()
			defer c.Close()

			key := benchmarkBatchKey(fmt.Sprintf("parallel-get-%d", w), 0)
			for {
				idx := ops.Add(1) - 1
				if idx >= uint64(b.N) {
					return
				}
				if _, err := c.Get(context.Background(), addr, cachewire.GetRequest{Key: key}); err != nil {
					failures.Add(1)
					return
				}
			}
		}(worker, client)
	}

	wg.Wait()
	if failures.Load() > 0 {
		b.Fatalf("parallel get failed: %d errors", failures.Load())
	}
}

func benchmarkServerParallelSetGet(b *testing.B, concurrency int) {
	b.Helper()

	addr, stopServer := startServerForConcurrencyBenchmark(b)
	b.Cleanup(stopServer)

	var ops atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(concurrency)

	b.ReportAllocs()
	b.ResetTimer()

	for worker := 0; worker < concurrency; worker++ {
		client, err := newBenchmarkFrameClientForConcurrency(addr)
		if err != nil {
			b.Fatalf("create benchmark frame client: %v", err)
		}
		go func(w int, c *benchmarkFrameClient) {
			defer wg.Done()
			defer c.Close()

			key := benchmarkBatchKey(fmt.Sprintf("parallel-setget-%d", w), 0)
			for {
				idx := ops.Add(1) - 1
				if idx >= uint64(b.N) {
					return
				}
				if _, err := c.Set(context.Background(), addr, cachewire.SetRequest{
					Key:       key,
					Value:     []byte("value"),
					TTLMillis: 60_000,
				}); err != nil {
					failures.Add(1)
					return
				}
				if _, err := c.Get(context.Background(), addr, cachewire.GetRequest{Key: key}); err != nil {
					failures.Add(1)
					return
				}
			}
		}(worker, client)
	}

	wg.Wait()
	if failures.Load() > 0 {
		b.Fatalf("parallel set/get failed: %d errors", failures.Load())
	}
}

func benchmarkServerParallelBatchSet(b *testing.B, concurrency int) {
	b.Helper()

	addr, stopServer := startServerForConcurrencyBenchmark(b)
	b.Cleanup(stopServer)

	const batchSize = 8
	var ops atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(concurrency)

	b.ReportAllocs()
	b.ResetTimer()

	for worker := 0; worker < concurrency; worker++ {
		client, err := newBenchmarkFrameClientForConcurrency(addr)
		if err != nil {
			b.Fatalf("create benchmark frame client: %v", err)
		}
		go func(w int, c *benchmarkFrameClient) {
			defer wg.Done()
			defer c.Close()

			request := cachewire.BatchSetRequest{
				Items: make([]cachewire.SetRequest, 0, batchSize),
			}
			for i := 0; i < batchSize; i++ {
				request.Items = append(request.Items, cachewire.SetRequest{
					Key:       benchmarkBatchKey(fmt.Sprintf("parallel-batch-%d", w), i),
					Value:     []byte("batch-value"),
					TTLMillis: 60_000,
				})
			}

			for {
				idx := ops.Add(1) - 1
				if idx >= uint64(b.N) {
					return
				}
				if _, err := c.BatchSet(context.Background(), addr, request); err != nil {
					failures.Add(1)
					return
				}
			}
		}(worker, client)
	}

	wg.Wait()
	if failures.Load() > 0 {
		b.Fatalf("parallel batch set failed: %d errors", failures.Load())
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

func newBenchmarkFrameClientForConcurrency(addr string) (*benchmarkFrameClient, error) {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("create benchmark tcp client: %w", err)
	}
	return &benchmarkFrameClient{
		conn:  conn,
		codec: protocol.NewCodec(),
	}, nil
}
