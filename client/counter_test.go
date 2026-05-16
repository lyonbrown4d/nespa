package client_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestTCPClientCounterCreatesAndIncrements(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	cacheClient, err := client.NewTCP(client.Config{Addr: server.Addr()})
	if err != nil {
		t.Fatalf("new tcp client: %v", err)
	}

	key := cachewire.Key{
		Namespace: "order",
		Space:     "session",
		Key:       "counter",
	}

	result, err := cacheClient.Counter(ctx, client.CounterRequest{
		Key:          key,
		InitialValue: 10,
		Delta:        5,
		TTLMillis:    15000,
	})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	if result.Value != 15 {
		t.Fatalf("counter value = %d, want %d", result.Value, 15)
	}
	if !result.Record.Found {
		t.Fatalf("counter record should be found")
	}

	transportClient := cachetcp.NewClient()
	record, err := transportClient.Get(ctx, server.Addr(), cachewire.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("get after counter: %v", err)
	}
	if !record.Found || string(record.Value) != "15" {
		t.Fatalf("record = %+v, want value 15", record)
	}
	if record.ExpireAtUnixMs <= 0 {
		t.Fatalf("record should have expiration with ttl")
	}
}

func TestTCPClientCounterRejectsNonIntValue(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	cacheClient, err := client.NewTCP(client.Config{Addr: server.Addr()})
	if err != nil {
		t.Fatalf("new tcp client: %v", err)
	}

	transportClient := cachetcp.NewClient()
	key := cachewire.Key{
		Namespace: "order",
		Space:     "session",
		Key:       "counter-non-int",
	}
	if _, seedErr := transportClient.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("abc"),
	}); seedErr != nil {
		t.Fatalf("seed set: %v", seedErr)
	}

	_, err = cacheClient.Counter(ctx, client.CounterRequest{
		Key:   key,
		Delta: 1,
	})
	if err == nil {
		t.Fatal("expected counter parse error")
	}

	record, err := transportClient.Get(ctx, server.Addr(), cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after counter fail: %v", err)
	}
	if !record.Found || string(record.Value) != "abc" {
		t.Fatalf("record should remain unchanged: %+v", record)
	}
}

func TestRoutedTCPClientCounterUsesRouteVersions(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	control := newSnapshotServer(t, snapshotFor(server.Addr(), 1, 1))
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}

	transportClient := cachetcp.NewClient()
	key := cachewire.Key{
		Namespace: "orders",
		Space:     "session",
		Key:       "counter-route",
	}
	seedRouteCounter(ctx, t, transportClient, server.Addr(), key)

	control.Set(snapshotFor(server.Addr(), 2, 2))
	if refreshErr := routed.Refresh(ctx); refreshErr != nil {
		t.Fatalf("refresh snapshot: %v", refreshErr)
	}

	result, err := routed.Counter(ctx, client.CounterRequest{
		Key:              key,
		InitialValue:     0,
		Delta:            1,
		TTLMillis:        10000,
		NamespaceVersion: 99,
		SpaceVersion:     99,
		MaxRetries:       3,
	})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	requireCounterResult(t, result, 1, 2, 2)

	record, err := transportClient.Get(ctx, server.Addr(), cachewire.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("get new version record: %v", err)
	}
	if !record.Found || string(record.Value) != "1" {
		t.Fatalf("record = %+v, want found value 1", record)
	}
}

func seedRouteCounter(ctx context.Context, t *testing.T, transportClient *cachetcp.Client, addr string, key cachewire.Key) {
	t.Helper()
	_, err := transportClient.Set(ctx, addr, cachewire.SetRequest{
		Key:              key,
		Value:            []byte("1"),
		NamespaceVersion: 1,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("seed set: %v", err)
	}
}

func requireCounterResult(t *testing.T, result client.CounterResult, value int64, namespaceVersion, spaceVersion uint64) {
	t.Helper()
	if result.Value != value {
		t.Fatalf("result value = %d, want %d", result.Value, value)
	}
	if result.Record.NamespaceVersion != namespaceVersion || result.Record.SpaceVersion != spaceVersion {
		t.Fatalf("record versions = %d/%d, want %d/%d", result.Record.NamespaceVersion, result.Record.SpaceVersion, namespaceVersion, spaceVersion)
	}
}
