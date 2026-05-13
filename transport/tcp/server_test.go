package tcp_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cacheapi"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerSetGetDelete(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	set, err := client.Set(ctx, server.Addr(), cacheapi.SetBody{Namespace: "ns", Space: "sp", Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if !set.Found || set.Version != 1 {
		t.Fatalf("unexpected set response: %+v", set)
	}

	get, err := client.Get(ctx, server.Addr(), cacheapi.GetInput{Namespace: "ns", Space: "sp", Key: "k"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !get.Found || get.Value != "v" {
		t.Fatalf("unexpected get response: %+v", get)
	}

	del, err := client.Delete(ctx, server.Addr(), cacheapi.DeleteInput{Namespace: "ns", Space: "sp", Key: "k"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !del.Deleted {
		t.Fatalf("unexpected delete response: %+v", del)
	}
}

func closeEngine(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func stopServer(t *testing.T, server *cachetcp.Server) {
	t.Helper()
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("stop tcp server: %v", err)
	}
}
