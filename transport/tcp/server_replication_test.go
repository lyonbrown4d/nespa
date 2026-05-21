package tcp_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerReplicatesSetToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "replicated"}
	source := startReplicatingCacheServer(t, target, key)

	_, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("set source: %v", err)
	}

	requireEventuallyWireValue(t, client, target.Addr(), key, "payload")
	requireEventuallyReplicationSequence(t, source, 1)
}

func TestClientServerReplicatesDeleteToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "deleted"}
	source := startReplicatingCacheServer(t, target, key)

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	requireEventuallyWireValue(t, client, target.Addr(), key, "payload")

	deleted, err := client.Delete(t.Context(), source.Addr(), cachewire.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("delete source: %v", err)
	}
	if !deleted.Deleted {
		t.Fatalf("delete source response = %+v, want deleted", deleted)
	}
	requireEventuallyWireMissing(t, client, target.Addr(), key)
}

func TestClientServerReplicatesTouchToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "touched"}
	source := startReplicatingCacheServer(t, target, key)

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:       key,
		Value:     []byte("payload"),
		TTLMillis: 5_000,
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	initial := requireEventuallyWireRecord(t, client, target.Addr(), key)

	touched, err := client.Touch(t.Context(), source.Addr(), cachewire.TouchRequest{
		Key:       key,
		TTLMillis: 10_000,
	})
	if err != nil {
		t.Fatalf("touch source: %v", err)
	}
	if !touched.Touched {
		t.Fatalf("touch source response = %+v, want touched", touched)
	}
	requireEventuallyReplicaExpireAfter(t, client, target.Addr(), key, initial.ExpireAtUnixMs+1_000)
}

func TestClientServerReplicatesAdjustToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "counter"}
	source := startReplicatingCacheServer(t, target, key)

	record, err := client.Adjust(t.Context(), source.Addr(), cachewire.AdjustRequest{
		Key:          key,
		InitialValue: 10,
		Delta:        5,
	})
	if err != nil {
		t.Fatalf("adjust source: %v", err)
	}
	if !record.Found || string(record.Value) != "15" {
		t.Fatalf("adjust source record = %+v, want 15", record)
	}
	requireEventuallyWireValue(t, client, target.Addr(), key, "15")
}

func TestClientServerReplicatesPrimitiveToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "primitive-counter"}
	source := startReplicatingCacheServer(t, target, key)

	result, err := client.Primitive(t.Context(), source.Addr(), cachewire.PrimitiveRequest{
		Key:          key,
		Kind:         cachewire.PrimitiveCounterAdjust,
		InitialValue: 20,
		Delta:        2,
	})
	if err != nil {
		t.Fatalf("primitive source: %v", err)
	}
	if !result.Applied || string(result.Value) != "22" {
		t.Fatalf("primitive source result = %+v, want applied 22", result)
	}
	requireEventuallyWireValue(t, client, target.Addr(), key, "22")
}

func startReplicatingCacheServer(t *testing.T, target *cachetcp.Server, keys ...cachewire.Key) *cachetcp.Server {
	t.Helper()
	return startCacheServer(t, cachetcp.ServerConfig{
		Addr: "127.0.0.1:0",
		ReplicaTargets: func(in cachewire.Key) []string {
			for index := range keys {
				if sameWireKey(in, keys[index]) {
					return []string{target.Addr()}
				}
			}
			return nil
		},
	})
}

func sameWireKey(left, right cachewire.Key) bool {
	return left.Namespace == right.Namespace &&
		left.Space == right.Space &&
		left.Entity == right.Entity &&
		left.Key == right.Key
}

func startCacheServer(t *testing.T, cfg cachetcp.ServerConfig) *cachetcp.Server {
	t.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	t.Cleanup(func() { closeEngine(t, eng) })

	server := cachetcp.NewServer(cfg, cache.NewService(eng))
	if err := server.Start(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	t.Cleanup(func() { stopServer(t, server) })

	return server
}

func requireEventuallyWireValue(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key, want string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachewire.Record
	for time.Now().Before(deadline) {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get replica: %v", err)
		}
		if record.Found && string(record.Value) == want {
			return
		}
		last = record
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica record = %+v, want value %q", last, want)
}

func requireEventuallyWireRecord(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key) cachewire.Record {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachewire.Record
	for time.Now().Before(deadline) {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get replica: %v", err)
		}
		if record.Found {
			return record
		}
		last = record
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica record = %+v, want found", last)
	return cachewire.Record{}
}

func requireEventuallyWireMissing(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachewire.Record
	for time.Now().Before(deadline) {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get replica: %v", err)
		}
		if !record.Found {
			return
		}
		last = record
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica record = %+v, want missing", last)
}

func requireEventuallyReplicaExpireAfter(
	t *testing.T,
	client *cachetcp.Client,
	addr string,
	key cachewire.Key,
	minExpireAtUnixMs int64,
) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachewire.Record
	for time.Now().Before(deadline) {
		record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("get replica: %v", err)
		}
		if record.Found && record.ExpireAtUnixMs > minExpireAtUnixMs {
			return
		}
		last = record
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica record = %+v, want expire_at > %d", last, minExpireAtUnixMs)
}

func requireEventuallyReplicationSequence(t *testing.T, server *cachetcp.Server, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachetcp.ReplicationStats
	for time.Now().Before(deadline) {
		last = server.ReplicationStats()
		if replicationStatsReachedSequence(last, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replication stats = %+v, want replicated sequence %d", last, want)
}

func replicationStatsReachedSequence(stats cachetcp.ReplicationStats, want uint64) bool {
	return stats.Enqueued == want &&
		stats.Attempts == want &&
		stats.Successes == want &&
		stats.LastQueuedSequence == want &&
		stats.LastAttemptSequence == want &&
		stats.LastSuccessSequence == want
}
