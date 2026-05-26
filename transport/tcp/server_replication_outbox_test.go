package tcp_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerAppendsReplicationOutboxEntry(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "outbox"}
	outboxPath := filepath.Join(t.TempDir(), "replication", "outbox.jsonl")
	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr:                  "127.0.0.1:0",
		ReplicationOutboxPath: outboxPath,
		ReplicaTargets: func(in cachewire.Key) []string {
			if sameWireKey(in, key) {
				return []string{target.Addr()}
			}
			return nil
		},
	})

	_, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("set source: %v", err)
	}

	requireEventuallyWireValue(t, client, target.Addr(), key, "payload")
	entry := requireReplicationOutboxEntry(t, outboxPath)
	assertSetOutboxEntry(t, entry, target.Addr(), key)
	requireReplicationAckOffset(t, outboxPath, target.Addr(), 1)
}

func TestClientServerResumesReplicationOutboxSequence(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "outbox-resume"}
	outboxPath := filepath.Join(t.TempDir(), "replication", "outbox.jsonl")
	writeReplicationOutboxSeed(t, outboxPath, 7)
	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr:                  "127.0.0.1:0",
		ReplicationOutboxPath: outboxPath,
		ReplicaTargets: func(in cachewire.Key) []string {
			if sameWireKey(in, key) {
				return []string{target.Addr()}
			}
			return nil
		},
	})

	_, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("set source: %v", err)
	}

	requireEventuallyWireValue(t, client, target.Addr(), key, "payload")
	entry := requireLastReplicationOutboxEntry(t, outboxPath, 2)
	if entry.Sequence != 8 {
		t.Fatalf("resumed outbox sequence = %d, want 8", entry.Sequence)
	}
	requireReplicationAckOffset(t, outboxPath, target.Addr(), 8)
}

func TestClientServerReplaysPendingReplicationOutboxEntriesOnStart(t *testing.T) {
	target, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "replay-set"}
	outboxPath := filepath.Join(t.TempDir(), "replication", "outbox.jsonl")
	replayPayload := []byte("replayed")

	writeReplicationOutboxEntry(t, outboxPath, replicationOutboxEntryForTest{
		Sequence: 10,
		Target:   target.Addr(),
		Kind:     1,
		Op:       protocol.OpCacheSet,
		Metadata: cachewire.EncodeSetRequest(cachewire.SetRequest{
			Key:              key,
			NamespaceVersion: 2,
			SpaceVersion:     3,
			ExpectedVersion:  5,
			TTLMillis:        30_000,
		}),
		Payload: replayPayload,
	})

	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr:                  "127.0.0.1:0",
		ReplicationOutboxPath: outboxPath,
		ReplicaTargets: func(in cachewire.Key) []string {
			if sameWireKey(in, key) {
				return []string{target.Addr()}
			}
			return nil
		},
	})

	requireEventuallyWireValue(t, client, target.Addr(), key, string(replayPayload))
	requireReplicationAckOffset(t, outboxPath, target.Addr(), 10)
	requireEventuallyReplayedSequence(t, source, 10)
}

func TestClientServerCatchesUpDroppedReplicationWritesFromOutboxReplay(t *testing.T) {
	targetAddr := reserveTCPAddr(t)
	outboxPath := filepath.Join(t.TempDir(), "replication", "outbox.jsonl")
	client := cachetcp.NewClient()
	const keyCount = 8

	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr:                  "127.0.0.1:0",
		ReplicationOutboxPath: outboxPath,
		ReplicationQueueSize:  1,
		ReplicaTargets: func(in cachewire.Key) []string {
			if in.Namespace == "orders" && in.Space == "session" {
				return []string{targetAddr}
			}
			return nil
		},
	})

	keys := make([]cachewire.Key, 0, keyCount)
	for index := range keyCount {
		key := cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       fmt.Sprintf("catchup-%d", index),
		}
		keys = append(keys, key)
		if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
			Key:   key,
			Value: fmt.Appendf(nil, "value-%d", index),
		}); err != nil {
			t.Fatalf("set source key %q: %v", key.Key, err)
		}
	}

	requireEventuallyDroppedReplications(t, source, 1)

	replica := startCacheServer(t, cachetcp.ServerConfig{Addr: targetAddr})
	for index := range keys {
		requireEventuallyWireValue(t, client, replica.Addr(), keys[index], fmt.Sprintf("value-%d", index))
	}
	requireReplicationAckOffset(t, outboxPath, targetAddr, keyCount)
}
