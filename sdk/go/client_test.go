package nespa_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	nespa "github.com/lyonbrown4d/nespa/sdk/go"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestDirectClientSetGetDelete(t *testing.T) {
	server := startSDKTCPServer(t)
	sdk := newDirectClient(t, server)
	key := nespa.Key{Namespace: "orders", Space: "session", Key: "direct-sdk"}

	record, err := sdk.Set(t.Context(), key, []byte("value"), nespa.SetOptions{TTL: time.Second})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	requireSDKRecord(t, record, "value")

	record, err = sdk.Get(t.Context(), key, nespa.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	requireSDKRecord(t, record, "value")

	exists, err := sdk.Exists(t.Context(), key, nespa.GetOptions{})
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("record should exist")
	}

	deleted, err := sdk.Delete(t.Context(), key, nespa.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("record should be deleted")
	}
}

func TestDirectClientAdjust(t *testing.T) {
	server := startSDKTCPServer(t)
	sdk := newDirectClient(t, server)
	key := nespa.Key{Namespace: "orders", Space: "session", Key: "counter"}

	record, err := sdk.Adjust(t.Context(), key, nespa.AdjustOptions{InitialValue: 10, Delta: 2})
	if err != nil {
		t.Fatalf("adjust create: %v", err)
	}
	requireSDKRecord(t, record, "12")

	record, err = sdk.Adjust(t.Context(), key, nespa.AdjustOptions{Delta: -3})
	if err != nil {
		t.Fatalf("adjust increment: %v", err)
	}
	requireSDKRecord(t, record, "9")
}

func TestDirectClientBatchGet(t *testing.T) {
	server := startSDKTCPServer(t)
	sdk := newDirectClient(t, server)
	key := nespa.Key{Namespace: "orders", Space: "session", Key: "batch-sdk"}

	records, err := sdk.BatchSet(t.Context(), []nespa.SetItem{{Key: key, Value: []byte("value")}})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("batch set records = %d, want 1", len(records))
	}

	records, err = sdk.BatchGet(t.Context(), []nespa.GetItem{{Key: key}})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("batch get records = %d, want 1", len(records))
	}
	requireSDKRecord(t, records[0], "value")
}

func TestDirectClientBatchDeleteExistsTouch(t *testing.T) {
	server := startSDKTCPServer(t)
	sdk := newDirectClient(t, server)
	key := nespa.Key{Namespace: "orders", Space: "session", Key: "batch-more-sdk"}

	if _, err := sdk.BatchSet(t.Context(), []nespa.SetItem{{Key: key, Value: []byte("value")}}); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	exists, err := sdk.BatchExists(t.Context(), []nespa.GetItem{{Key: key}})
	if err != nil {
		t.Fatalf("batch exists: %v", err)
	}
	requireSDKBoolResults(t, exists, true, "batch exists")

	touched, err := sdk.BatchTouch(t.Context(), []nespa.TouchItem{{Key: key, Options: nespa.TouchOptions{TTL: time.Second}}})
	if err != nil {
		t.Fatalf("batch touch: %v", err)
	}
	requireSDKBoolResults(t, touched, true, "batch touch")

	deleted, err := sdk.BatchDelete(t.Context(), []nespa.DeleteItem{{Key: key}})
	if err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	requireSDKBoolResults(t, deleted, true, "batch delete")
}

func TestDirectClientBatchPrimitive(t *testing.T) {
	server := startSDKTCPServer(t)
	sdk := newDirectClient(t, server)

	results, err := sdk.BatchPrimitive(t.Context(), []nespa.PrimitiveRequest{
		{Kind: nespa.PrimitiveCounterAdjust, Key: sdkPrimitiveKey("counter"), Delta: 1},
		{Kind: nespa.PrimitiveMapSet, Key: sdkPrimitiveKey("profile"), Field: "name", Value: []byte("alice")},
		{Kind: nespa.PrimitiveSetAdd, Key: sdkPrimitiveKey("tags"), Member: "blue"},
		{Kind: nespa.PrimitiveScoredSetPut, Key: sdkPrimitiveKey("rank"), Member: "alice", Score: 10},
		{Kind: nespa.PrimitiveMapGet, Key: sdkPrimitiveKey("profile"), Field: "name"},
		{Kind: nespa.PrimitiveSetContains, Key: sdkPrimitiveKey("tags"), Member: "blue"},
		{Kind: nespa.PrimitiveScoredSetRange, Key: sdkPrimitiveKey("rank")},
	})
	if err != nil {
		t.Fatalf("batch primitive: %v", err)
	}
	requireSDKPrimitiveResults(t, results)
}

func requireSDKBoolResults(t *testing.T, results []bool, want bool, name string) {
	t.Helper()
	if len(results) != 1 || results[0] != want {
		t.Fatalf("%s results = %+v, want [%v]", name, results, want)
	}
}

func TestErrorCodeOf(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", cachewire.Error{Code: nespa.ErrorInvalidArgument, Message: "bad counter"})

	code, ok := nespa.ErrorCodeOf(err)
	if !ok {
		t.Fatal("expected wire error code")
	}
	if code != nespa.ErrorInvalidArgument {
		t.Fatalf("code = %d, want %d", code, nespa.ErrorInvalidArgument)
	}

	if _, ok := nespa.ErrorCodeOf(errors.New("plain")); ok {
		t.Fatal("plain error should not expose a wire error code")
	}
}

func requireSDKPrimitiveResults(t *testing.T, results []nespa.PrimitiveResult) {
	t.Helper()
	if len(results) != 7 {
		t.Fatalf("primitive results len = %d, want 7", len(results))
	}
	if string(results[0].Value) != "1" {
		t.Fatalf("counter result = %+v", results[0])
	}
	if !results[4].Found || string(results[4].Value) != "alice" {
		t.Fatalf("map get result = %+v", results[4])
	}
	if !results[5].Bool {
		t.Fatalf("set contains result = %+v", results[5])
	}
	if len(results[6].ScoredMembers) != 1 || results[6].ScoredMembers[0].Member != "alice" {
		t.Fatalf("scored range result = %+v", results[6])
	}
}

func sdkPrimitiveKey(key string) nespa.Key {
	return nespa.Key{Namespace: "orders", Space: "session", Key: key}
}

func newDirectClient(t *testing.T, server *cachetcp.Server) *nespa.Client {
	t.Helper()
	sdk, err := nespa.NewDirect(server.Addr())
	if err != nil {
		t.Fatalf("new direct: %v", err)
	}
	return sdk
}

func startSDKTCPServer(t *testing.T) *cachetcp.Server {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() {
		if err := eng.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	})

	server := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Fatalf("stop tcp server: %v", err)
		}
	})
	return server
}

func requireSDKRecord(t *testing.T, record nespa.Record, value string) {
	t.Helper()
	if !record.Found || string(record.Value) != value {
		t.Fatalf("record = %+v, want value %q", record, value)
	}
}
