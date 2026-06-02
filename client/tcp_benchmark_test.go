package client_test

import (
	"context"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func BenchmarkTCPClientSetGetRoundTrip(b *testing.B) {
	cacheClient := startDirectTCPClientBenchmark(b)
	ctx := context.Background()
	key := cachewire.Key{Namespace: "bench", Space: "session", Key: "direct-set-get"}
	value := []byte("value")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cacheClient.Set(ctx, cachewire.SetRequest{Key: key, Value: value}); err != nil {
			b.Fatalf("set: %v", err)
		}
		record, err := cacheClient.Get(ctx, cachewire.GetRequest{Key: key})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if !record.Found || string(record.Value) != "value" {
			b.Fatalf("record = %+v, want value", record)
		}
	}
}

func BenchmarkTCPClientBatchSetGet(b *testing.B) {
	cacheClient := startDirectTCPClientBenchmark(b)
	ctx := context.Background()
	setRequest := cachewire.BatchSetRequest{Items: benchmarkClientSetItems(16)}
	getRequest := cachewire.BatchGetRequest{Items: benchmarkClientGetItems(16)}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cacheClient.BatchSet(ctx, setRequest); err != nil {
			b.Fatalf("batch set: %v", err)
		}
		response, err := cacheClient.BatchGet(ctx, getRequest)
		if err != nil {
			b.Fatalf("batch get: %v", err)
		}
		if len(response.Records) != 16 || !response.Records[0].Found {
			b.Fatalf("batch get records = %+v", response.Records)
		}
	}
}

func startDirectTCPClientBenchmark(b *testing.B) *client.TCPClient {
	b.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		b.Fatalf("start tcp server: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			b.Fatalf("stop tcp server: %v", err)
		}
		if err := eng.Close(); err != nil {
			b.Fatalf("close engine: %v", err)
		}
	})

	cacheClient, err := client.NewTCP(client.Config{Addr: server.Addr()})
	if err != nil {
		b.Fatalf("new tcp client: %v", err)
	}
	return cacheClient
}

func benchmarkClientSetItems(count int) []cachewire.SetRequest {
	items := make([]cachewire.SetRequest, 0, count)
	for index := range count {
		items = append(items, cachewire.SetRequest{
			Key:   benchmarkClientKey(index),
			Value: []byte("benchmark-value-" + strconv.Itoa(index)),
		})
	}
	return items
}

func benchmarkClientGetItems(count int) []cachewire.GetRequest {
	items := make([]cachewire.GetRequest, 0, count)
	for index := range count {
		items = append(items, cachewire.GetRequest{Key: benchmarkClientKey(index)})
	}
	return items
}

func benchmarkClientKey(index int) cachewire.Key {
	return cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Entity:    "direct",
		Key:       "k-" + strconv.Itoa(index),
	}
}
