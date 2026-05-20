package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerReplicatesBatchSetToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	keys := replicationBatchKeys("batch-set")
	source := startReplicatingCacheServer(t, target, keys...)

	_, err := client.BatchSet(t.Context(), source.Addr(), cachewire.BatchSetRequest{
		Items: []cachewire.SetRequest{
			{Key: keys[0], Value: []byte("alpha")},
			{Key: keys[1], Value: []byte("beta")},
		},
	})
	if err != nil {
		t.Fatalf("batch set source: %v", err)
	}
	requireEventuallyWireValue(t, client, target.Addr(), keys[0], "alpha")
	requireEventuallyWireValue(t, client, target.Addr(), keys[1], "beta")
}

func TestClientServerReplicatesBatchDeleteToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	keys := replicationBatchKeys("batch-delete")
	source := startReplicatingCacheServer(t, target, keys...)

	seedReplicatedBatch(t, client, source.Addr(), target.Addr(), keys)
	deleted, err := client.BatchDelete(t.Context(), source.Addr(), cachewire.BatchDeleteRequest{
		Items: []cachewire.DeleteRequest{{Key: keys[0]}, {Key: keys[1]}},
	})
	if err != nil {
		t.Fatalf("batch delete source: %v", err)
	}
	if len(deleted.Results) != 2 || !deleted.Results[0].Deleted || !deleted.Results[1].Deleted {
		t.Fatalf("batch delete source response = %+v, want both deleted", deleted)
	}
	requireEventuallyWireMissing(t, client, target.Addr(), keys[0])
	requireEventuallyWireMissing(t, client, target.Addr(), keys[1])
}

func TestClientServerReplicatesBatchTouchToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	keys := replicationBatchKeys("batch-touch")
	source := startReplicatingCacheServer(t, target, keys...)

	seedReplicatedBatch(t, client, source.Addr(), target.Addr(), keys)
	initial := requireEventuallyWireRecord(t, client, target.Addr(), keys[0])

	touched, err := client.BatchTouch(t.Context(), source.Addr(), cachewire.BatchTouchRequest{
		Items: []cachewire.TouchRequest{
			{Key: keys[0], TTLMillis: 10_000},
			{Key: keys[1], TTLMillis: 10_000},
		},
	})
	if err != nil {
		t.Fatalf("batch touch source: %v", err)
	}
	if len(touched.Results) != 2 || !touched.Results[0].Touched || !touched.Results[1].Touched {
		t.Fatalf("batch touch source response = %+v, want both touched", touched)
	}
	requireEventuallyReplicaExpireAfter(t, client, target.Addr(), keys[0], initial.ExpireAtUnixMs+1_000)
}

func TestClientServerReplicatesBatchPrimitiveToReplica(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "batch-primitive-counter"}
	source := startReplicatingCacheServer(t, target, key)

	result, err := client.BatchPrimitive(t.Context(), source.Addr(), cachewire.BatchPrimitiveRequest{
		Items: []cachewire.PrimitiveRequest{
			{Key: key, Kind: cachewire.PrimitiveCounterAdjust, InitialValue: 1, Delta: 2},
			{Key: key, Kind: cachewire.PrimitiveCounterAdjust, Delta: 3},
		},
	})
	if err != nil {
		t.Fatalf("batch primitive source: %v", err)
	}
	if len(result.Results) != 2 || !result.Results[0].Applied || !result.Results[1].Applied {
		t.Fatalf("batch primitive source response = %+v, want both applied", result)
	}
	requireEventuallyWireValue(t, client, target.Addr(), key, "6")
}

func replicationBatchKeys(suffix string) []cachewire.Key {
	return []cachewire.Key{
		{Namespace: "orders", Space: "session", Key: suffix + "-a"},
		{Namespace: "orders", Space: "session", Key: suffix + "-b"},
	}
}

func seedReplicatedBatch(t *testing.T, client *cachetcp.Client, sourceAddr, targetAddr string, keys []cachewire.Key) {
	t.Helper()
	_, err := client.BatchSet(t.Context(), sourceAddr, cachewire.BatchSetRequest{
		Items: []cachewire.SetRequest{
			{Key: keys[0], Value: []byte("alpha"), TTLMillis: 5_000},
			{Key: keys[1], Value: []byte("beta"), TTLMillis: 5_000},
		},
	})
	if err != nil {
		t.Fatalf("seed batch source: %v", err)
	}
	requireEventuallyWireValue(t, client, targetAddr, keys[0], "alpha")
	requireEventuallyWireValue(t, client, targetAddr, keys[1], "beta")
}
