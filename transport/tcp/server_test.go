package tcp_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
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
	set, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:   cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"},
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if !set.Found || set.Version != 1 {
		t.Fatalf("unexpected set response: %+v", set)
	}

	get, err := client.Get(ctx, server.Addr(), cachewire.GetRequest{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"}})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !get.Found || string(get.Value) != "v" {
		t.Fatalf("unexpected get response: %+v", get)
	}

	del, err := client.Delete(ctx, server.Addr(), cachewire.DeleteRequest{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"}})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !del.Deleted {
		t.Fatalf("unexpected delete response: %+v", del)
	}
}

func TestClientServerExpectedVersionEnforced(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"}
	set, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	stale, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:             key,
		Value:           []byte("updated"),
		ExpectedVersion: set.Version + 1,
	})
	if err != nil {
		t.Fatalf("stale set: %v", err)
	}
	if stale.Found {
		t.Fatalf("stale set should not match: %+v", stale)
	}

	deleted, err := client.Delete(ctx, server.Addr(), cachewire.DeleteRequest{
		Key:             key,
		ExpectedVersion: set.Version + 1,
	})
	if err != nil {
		t.Fatalf("stale delete: %v", err)
	}
	if deleted.Deleted {
		t.Fatalf("stale delete should not apply: %+v", deleted)
	}

	touched, err := client.Touch(ctx, server.Addr(), cachewire.TouchRequest{
		Key:             key,
		ExpectedVersion: set.Version + 1,
	})
	if err != nil {
		t.Fatalf("stale touch: %v", err)
	}
	if touched.Touched {
		t.Fatalf("stale touch should not apply: %+v", touched)
	}
}

func TestClientServerExistsHonorsVersions(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"}
	if _, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:              key,
		Value:            []byte("v"),
		NamespaceVersion: 1,
		SpaceVersion:     1,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	exists, err := client.Exists(ctx, server.Addr(), cachewire.ExistsRequest{
		Key:              key,
		NamespaceVersion: 1,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("exists matching version: %v", err)
	}
	if !exists.Exists {
		t.Fatalf("exists matching version = %+v, want true", exists)
	}

	exists, err = client.Exists(ctx, server.Addr(), cachewire.ExistsRequest{
		Key:              key,
		NamespaceVersion: 2,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("exists mismatched version: %v", err)
	}
	if exists.Exists {
		t.Fatalf("exists mismatched version = %+v, want false", exists)
	}
}

func TestClientServerTouchHonorsVersions(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"}
	if _, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:              key,
		Value:            []byte("v"),
		NamespaceVersion: 1,
		SpaceVersion:     1,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	touched, err := client.Touch(ctx, server.Addr(), cachewire.TouchRequest{
		Key:              key,
		TTLMillis:        1000,
		NamespaceVersion: 2,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("touch mismatched version: %v", err)
	}
	if touched.Touched {
		t.Fatalf("touch mismatched version = %+v, want false", touched)
	}

	touched, err = client.Touch(ctx, server.Addr(), cachewire.TouchRequest{
		Key:              key,
		TTLMillis:        1000,
		NamespaceVersion: 1,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("touch matching version: %v", err)
	}
	if !touched.Touched {
		t.Fatalf("touch matching version = %+v, want true", touched)
	}
}

func TestClientServerAdjustIncrementsAndPreservesTTL(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "counter"}

	record, err := client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: 10,
		Delta:        7,
		TTLMillis:    5000,
	})
	if err != nil {
		t.Fatalf("adjust create: %v", err)
	}
	if !record.Found || string(record.Value) != "17" || record.Version != 1 || record.ExpireAtUnixMs <= 0 {
		t.Fatalf("adjust create record = %+v", record)
	}

	record, err = client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:   key,
		Delta: -2,
	})
	if err != nil {
		t.Fatalf("adjust increment: %v", err)
	}
	if !record.Found || string(record.Value) != "15" || record.Version != 2 {
		t.Fatalf("adjust increment record = %+v", record)
	}
	if record.ExpireAtUnixMs <= 0 {
		t.Fatalf("adjust should preserve TTL: %+v", record)
	}
}

func TestClientServerAdjustRejectsMismatchedExpectedVersion(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "counter"}

	seed, err := client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: 1,
		Delta:        1,
	})
	if err != nil {
		t.Fatalf("seed adjust: %v", err)
	}

	missed, err := client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:             key,
		Delta:           1,
		ExpectedVersion: seed.Version + 1,
	})
	if err != nil {
		t.Fatalf("adjust with mismatch: %v", err)
	}
	if missed.Found {
		t.Fatalf("adjust with mismatched version should not apply: %+v", missed)
	}

	record, err := client.Get(ctx, server.Addr(), cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after mismatched adjust: %v", err)
	}
	if string(record.Value) != "2" {
		t.Fatalf("record changed after mismatched adjust: %+v", record)
	}
}

func TestClientServerAdjustRejectsInvalidValue(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "bad"}
	if _, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("bad-int"),
	}); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	_, err := client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:   key,
		Delta: 1,
	})
	if err == nil {
		t.Fatal("expected adjust invalid value error")
	}
	wireErr, ok := errors.AsType[cachewire.Error](err)
	if !ok {
		t.Fatalf("expected cachewire.Error, got %T: %v", err, err)
	}
	if wireErr.Code != protocol.ErrorInvalidArgument {
		t.Fatalf("unexpected error code: %d", wireErr.Code)
	}
}

func TestClientServerAdjustRejectsOverflow(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "overflow"}
	maxInt64 := int64(^uint64(0) >> 1)

	_, err := client.Adjust(ctx, server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: maxInt64,
		Delta:        1,
	})
	if err == nil {
		t.Fatal("expected adjust overflow error")
	}
	wireErr, ok := errors.AsType[cachewire.Error](err)
	if !ok {
		t.Fatalf("expected cachewire.Error, got %T: %v", err, err)
	}
	if wireErr.Code != protocol.ErrorInvalidArgument {
		t.Fatalf("unexpected error code: %d", wireErr.Code)
	}
}

func TestClientServerRejectsStaleRouteEpoch(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr:              "127.0.0.1:0",
		CurrentRouteEpoch: func() uint64 { return 2 },
	}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	_, err := client.Get(ctx, server.Addr(), cachewire.GetRequest{
		Key:        cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"},
		RouteEpoch: 1,
	})
	if err == nil {
		t.Fatal("expected stale route epoch error")
	}
	wireErr, ok := errors.AsType[cachewire.Error](err)
	if !ok {
		t.Fatalf("expected cachewire.Error, got %T", err)
	}
	if wireErr.Code != protocol.ErrorNoRoute {
		t.Fatalf("unexpected error code: %d", wireErr.Code)
	}
}

func TestClientServerBatchSetGet(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	set, err := client.BatchSet(ctx, server.Addr(), cachewire.BatchSetRequest{
		Items: []cachewire.SetRequest{
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "a"}, Value: []byte("alpha")},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "b"}, Value: []byte("beta")},
		},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(set.Records) != 2 || set.Records[0].Version != 1 || set.Records[1].Version != 1 {
		t.Fatalf("unexpected batch set response: %+v", set)
	}

	get, err := client.BatchGet(ctx, server.Addr(), cachewire.BatchGetRequest{
		Items: []cachewire.GetRequest{
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "a"}},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "missing"}},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "b"}},
		},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	requireBatchGet(t, get)
}

func requireBatchGet(t *testing.T, get cachewire.BatchGetResponse) {
	t.Helper()
	if len(get.Records) != 3 {
		t.Fatalf("unexpected batch get count: %+v", get)
	}
	if !get.Records[0].Found || string(get.Records[0].Value) != "alpha" {
		t.Fatalf("unexpected first batch get record: %+v", get.Records[0])
	}
	if get.Records[1].Found {
		t.Fatalf("unexpected missing batch get record: %+v", get.Records[1])
	}
	if !get.Records[2].Found || string(get.Records[2].Value) != "beta" {
		t.Fatalf("unexpected second batch get record: %+v", get.Records[2])
	}
}

func TestClientServerInvalidKeyReturnsProtocolError(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := server.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, server)

	client := cachetcp.NewClient()
	_, err := client.Set(ctx, server.Addr(), cachewire.SetRequest{
		Key:   cachewire.Key{Namespace: "ns", Space: "sp"},
		Value: []byte("v"),
	})
	if err == nil {
		t.Fatal("expected invalid key error")
	}
	wireErr, ok := errors.AsType[cachewire.Error](err)
	if !ok {
		t.Fatalf("expected cachewire.Error, got %T", err)
	}
	if wireErr.Code != protocol.ErrorBadFrame {
		t.Fatalf("unexpected error code: %d", wireErr.Code)
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
