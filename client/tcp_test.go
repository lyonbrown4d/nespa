package client_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestNewTCPRejectsBlankAddr(t *testing.T) {
	_, err := client.NewTCP(client.Config{Addr: " \t "})
	if !errors.Is(err, client.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
}

func TestTCPClientSetGetBatchAndDelete(t *testing.T) {
	server, cacheClient := startDirectTCPClient(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "direct"}
	requireDirectSetGet(t, cacheClient, key)
	requireDirectBatchSetGet(t, cacheClient)
	requireDirectDelete(t, cacheClient, key)

	if server.Addr() == "" {
		t.Fatal("server addr should be available")
	}
}

func requireDirectSetGet(t *testing.T, cacheClient *client.TCPClient, key cachewire.Key) {
	t.Helper()
	ctx := context.Background()
	set, err := cacheClient.Set(ctx, cachewire.SetRequest{Key: key, Value: []byte("v1")})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if !set.Found || set.Version != 1 {
		t.Fatalf("set record = %+v, want found version 1", set)
	}

	record, err := cacheClient.Get(ctx, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !record.Found || string(record.Value) != "v1" {
		t.Fatalf("record = %+v, want value v1", record)
	}
}

func requireDirectBatchSetGet(t *testing.T, cacheClient *client.TCPClient) {
	t.Helper()
	ctx := context.Background()
	batchSet, err := cacheClient.BatchSet(ctx, cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "batch-a"}, Value: []byte("alpha")},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "batch-b"}, Value: []byte("beta")},
	}})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(batchSet.Records) != 2 {
		t.Fatalf("batch set records len = %d, want 2", len(batchSet.Records))
	}

	batchGet, err := cacheClient.BatchGet(ctx, cachewire.BatchGetRequest{Items: []cachewire.GetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "batch-a"}},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "missing"}},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "batch-b"}},
	}})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	requireDirectBatchGet(t, batchGet)
}

func requireDirectDelete(t *testing.T, cacheClient *client.TCPClient, key cachewire.Key) {
	t.Helper()
	ctx := context.Background()
	deleted, err := cacheClient.Delete(ctx, cachewire.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Deleted {
		t.Fatalf("deleted = false, want true")
	}

	miss, err := cacheClient.Get(ctx, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if miss.Found {
		t.Fatalf("record after delete = %+v, want missing", miss)
	}
}

func startDirectTCPClient(t *testing.T) (*cachetcp.Server, *client.TCPClient) {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() {
		if err := eng.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	})

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	t.Cleanup(func() {
		stopServer(t, server)
	})

	cacheClient, err := client.NewTCP(client.Config{Addr: server.Addr()})
	if err != nil {
		t.Fatalf("new tcp client: %v", err)
	}
	return server, cacheClient
}

func requireDirectBatchGet(t *testing.T, response cachewire.BatchGetResponse) {
	t.Helper()
	if len(response.Records) != 3 {
		t.Fatalf("records len = %d, want 3", len(response.Records))
	}
	if !response.Records[0].Found || string(response.Records[0].Value) != "alpha" {
		t.Fatalf("record[0] = %+v, want alpha", response.Records[0])
	}
	if response.Records[1].Found {
		t.Fatalf("record[1] = %+v, want missing", response.Records[1])
	}
	if !response.Records[2].Found || string(response.Records[2].Value) != "beta" {
		t.Fatalf("record[2] = %+v, want beta", response.Records[2])
	}
}
