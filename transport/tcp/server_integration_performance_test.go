package tcp_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerCRUDIntegration(t *testing.T) {
	target, client := startCacheClientServer(t)
	keyBase := cachewire.Key{
		Namespace: "integration",
		Space:     "session",
		Entity:    "user",
	}

	keyCount := 100
	keys := integrationKeys(keyBase, keyCount)

	seedIntegrationRecords(t, client, target.Addr(), keys)
	requireIntegrationRecords(t, client, target.Addr(), keys)
	requireIntegrationExists(t, client, target.Addr(), keys[:keyCount/2], true, "before delete")
	deleteIntegrationRecords(t, client, target.Addr(), keys[:keyCount/2])
	requireIntegrationMissing(t, client, target.Addr(), keys[:keyCount/2])
	requireIntegrationExists(t, client, target.Addr(), keys[keyCount/2:], true, "after delete")
}

func integrationKeys(base cachewire.Key, count int) []cachewire.Key {
	keys := make([]cachewire.Key, 0, count)
	for i := range count {
		key := base
		key.Key = "k-" + strconv.Itoa(i)
		keys = append(keys, key)
	}
	return keys
}

func seedIntegrationRecords(t *testing.T, client *cachetcp.Client, addr string, keys []cachewire.Key) {
	t.Helper()

	for index, key := range keys {
		seed := "value-" + strconv.Itoa(index)
		_, err := client.Set(t.Context(), addr, cachewire.SetRequest{
			Key:       key,
			Value:     []byte(seed),
			TTLMillis: 10_000,
		})
		if err != nil {
			t.Fatalf("set %q: %v", key.Key, err)
		}
	}
}

func requireIntegrationRecords(t *testing.T, client *cachetcp.Client, addr string, keys []cachewire.Key) {
	t.Helper()

	for index, key := range keys {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get %q: %v", key.Key, err)
		}
		if !record.Found {
			t.Fatalf("get %q: not found", key.Key)
		}
		want := "value-" + strconv.Itoa(index)
		if string(record.Value) != want {
			t.Fatalf("get %q: %q, want %q", key.Key, string(record.Value), want)
		}
	}
}

func requireIntegrationExists(
	t *testing.T,
	client *cachetcp.Client,
	addr string,
	keys []cachewire.Key,
	want bool,
	phase string,
) {
	t.Helper()

	for _, key := range keys {
		exists, err := client.Exists(t.Context(), addr, cachewire.ExistsRequest{Key: key})
		if err != nil {
			t.Fatalf("exists %q %s: %v", key.Key, phase, err)
		}
		if exists.Exists != want {
			t.Fatalf("exists %q %s = %t, want %t", key.Key, phase, exists.Exists, want)
		}
	}
}

func deleteIntegrationRecords(t *testing.T, client *cachetcp.Client, addr string, keys []cachewire.Key) {
	t.Helper()

	for _, key := range keys {
		deleted, err := client.Delete(t.Context(), addr, cachewire.DeleteRequest{Key: key})
		if err != nil {
			t.Fatalf("delete %q: %v", key.Key, err)
		}
		if !deleted.Deleted {
			t.Fatalf("delete %q: not deleted", key.Key)
		}
	}
}

func requireIntegrationMissing(t *testing.T, client *cachetcp.Client, addr string, keys []cachewire.Key) {
	t.Helper()

	for _, key := range keys {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get deleted %q: %v", key.Key, err)
		}
		if record.Found {
			t.Fatalf("get deleted %q: found", key.Key)
		}
	}
}

func BenchmarkServerSetGetRoundTrip(b *testing.B) {
	server, client := startServerForBenchmark(b)
	addr := server.Addr()
	key := cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Key:       "set-get-roundtrip",
	}
	value := []byte("value")
	want := string(value)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.Set(context.Background(), addr, cachewire.SetRequest{
			Key:   key,
			Value: value,
		}); err != nil {
			b.Fatalf("set: %v", err)
		}
		record, err := client.Get(context.Background(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if !record.Found || string(record.Value) != want {
			b.Fatalf("record = %+v, want found=%t value=%q", record, true, want)
		}
	}
}

func BenchmarkServerGetHit(b *testing.B) {
	server, client := startServerForBenchmark(b)
	addr := server.Addr()
	key := cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Key:       "get-hit",
	}

	if _, err := client.Set(context.Background(), addr, cachewire.SetRequest{
		Key:   key,
		Value: []byte("value"),
	}); err != nil {
		b.Fatalf("seed set: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		record, err := client.Get(context.Background(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if !record.Found {
			b.Fatalf("record = %+v, want found", record)
		}
	}
}

func BenchmarkServerDeleteHitRateAfterWarmup(b *testing.B) {
	server, client := startServerForBenchmark(b)
	addr := server.Addr()
	key := cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Key:       "delete",
	}

	for i := range 10 {
		if _, err := client.Set(context.Background(), addr, cachewire.SetRequest{
			Key:   key,
			Value: []byte("to-delete"),
		}); err != nil {
			b.Fatalf("seed set %d: %v", i, err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := client.Delete(context.Background(), addr, cachewire.DeleteRequest{Key: key}); err != nil {
			b.Fatalf("delete: %v", err)
		}
		if _, err := client.Set(context.Background(), addr, cachewire.SetRequest{
			Key:   key,
			Value: []byte("to-delete"),
		}); err != nil {
			b.Fatalf("set after delete %d: %v", i, err)
		}
	}
}

func BenchmarkServerBatchSet(b *testing.B) {
	server, client := startServerForBenchmark(b)
	addr := server.Addr()

	request := cachewire.BatchSetRequest{
		Items: batchSetItems(16),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.BatchSet(context.Background(), addr, request); err != nil {
			b.Fatalf("batch set: %v", err)
		}
	}
}

func BenchmarkServerBatchDelete(b *testing.B) {
	server, client := startServerForBenchmark(b)
	addr := server.Addr()

	seed := cachewire.BatchSetRequest{
		Items: batchSetItems(16),
	}
	if _, err := client.BatchSet(context.Background(), addr, seed); err != nil {
		b.Fatalf("seed batch set: %v", err)
	}

	request := cachewire.BatchDeleteRequest{
		Items: batchDeleteRequests(16),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := client.BatchDelete(context.Background(), addr, request); err != nil {
			b.Fatalf("batch delete: %v", err)
		}
		if _, err := client.BatchSet(context.Background(), addr, seed); err != nil {
			b.Fatalf("batch set after delete %d: %v", i, err)
		}
	}
}
