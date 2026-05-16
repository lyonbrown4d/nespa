package tcp_test

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerAdjustIncrementsAndPreservesTTL(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "counter"}

	record, err := client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: 10,
		Delta:        7,
		TTLMillis:    5000,
	})
	if err != nil {
		t.Fatalf("adjust create: %v", err)
	}
	assertWireAdjustRecord(t, record, "17", 1, true, "create")

	record, err = client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
		Key:   key,
		Delta: -2,
	})
	if err != nil {
		t.Fatalf("adjust increment: %v", err)
	}
	assertWireAdjustRecord(t, record, "15", 2, true, "increment")
}

func TestClientServerAdjustRejectsMismatchedExpectedVersion(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "counter"}

	seed, err := client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: 1,
		Delta:        1,
	})
	if err != nil {
		t.Fatalf("seed adjust: %v", err)
	}

	missed, err := client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
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
	requireServerValue(t, client, server, key, "2")
}

func TestClientServerAdjustRejectsInvalidValue(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "bad"}
	if _, err := client.Set(t.Context(), server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("bad-int"),
	}); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	_, err := client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
		Key:   key,
		Delta: 1,
	})
	requireWireErrorCode(t, err, protocol.ErrorInvalidArgument)
}

func TestClientServerAdjustRejectsOverflow(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "ns", Space: "sp", Key: "overflow"}
	maxInt64 := int64(^uint64(0) >> 1)

	_, err := client.Adjust(t.Context(), server.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: maxInt64,
		Delta:        1,
	})
	requireWireErrorCode(t, err, protocol.ErrorInvalidArgument)
}

func startCacheClientServer(t *testing.T) (*cachetcp.Server, *cachetcp.Client) {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeEngine(t, eng) })

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	t.Cleanup(func() { stopServer(t, server) })

	return server, cachetcp.NewClient()
}

func assertWireAdjustRecord(t *testing.T, record cachewire.Record, value string, version uint64, wantTTL bool, name string) {
	t.Helper()
	if !record.Found {
		t.Fatalf("adjust %s should be found", name)
	}
	if string(record.Value) != value {
		t.Fatalf("adjust %s value = %q, want %s", name, record.Value, value)
	}
	if record.Version != version {
		t.Fatalf("adjust %s version = %d, want %d", name, record.Version, version)
	}
	if (record.ExpireAtUnixMs > 0) != wantTTL {
		t.Fatalf("adjust %s ttl presence = %v, want %v", name, record.ExpireAtUnixMs > 0, wantTTL)
	}
}

func requireServerValue(t *testing.T, client *cachetcp.Client, server *cachetcp.Server, key cachewire.Key, want string) {
	t.Helper()
	record, err := client.Get(t.Context(), server.Addr(), cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after mismatched adjust: %v", err)
	}
	if string(record.Value) != want {
		t.Fatalf("record changed after mismatched adjust: %+v", record)
	}
}

func requireWireErrorCode(t *testing.T, err error, want protocol.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatal("expected wire error")
	}
	wireErr, ok := errors.AsType[cachewire.Error](err)
	if !ok {
		t.Fatalf("expected cachewire.Error, got %T: %v", err, err)
	}
	if wireErr.Code != want {
		t.Fatalf("unexpected error code: %d", wireErr.Code)
	}
}
