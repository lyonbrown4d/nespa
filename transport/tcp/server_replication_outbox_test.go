package tcp_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

type replicationOutboxEntryForTest struct {
	Sequence uint64      `json:"sequence"`
	Target   string      `json:"target"`
	Kind     uint8       `json:"kind"`
	Op       protocol.Op `json:"op"`
	Metadata []byte      `json:"metadata"`
	Payload  []byte      `json:"payload"`
}

type replicationAckStateForTest struct {
	Offsets map[string]uint64 `json:"offsets"`
}

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
	for index := 0; index < keyCount; index++ {
		key := cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Key:       fmt.Sprintf("catchup-%d", index),
		}
		keys = append(keys, key)
		if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
			Key:   key,
			Value: []byte(fmt.Sprintf("value-%d", index)),
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

func requireEventuallyReplayedSequence(t *testing.T, server *cachetcp.Server, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := server.ReplicationStats()
		if stats.Enqueued == 1 &&
			stats.Attempts == 1 &&
			stats.Successes == 1 &&
			stats.LastQueuedSequence == want &&
			stats.LastAttemptSequence == want &&
			stats.LastSuccessSequence == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	last := server.ReplicationStats()
	t.Fatalf("replication replay stats = %+v, want sequence %d", last, want)
}

func requireReplicationOutboxEntry(t *testing.T, path string) replicationOutboxEntryForTest {
	t.Helper()
	entries := requireReplicationOutboxEntries(t, path, 1)
	return entries[0]
}

func requireLastReplicationOutboxEntry(t *testing.T, path string, wantCount int) replicationOutboxEntryForTest {
	t.Helper()
	entries := requireReplicationOutboxEntries(t, path, wantCount)
	return entries[len(entries)-1]
}

func requireReplicationOutboxEntries(
	t *testing.T,
	path string,
	wantCount int,
) []replicationOutboxEntryForTest {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		entries, err := readReplicationOutboxEntries(path)
		if err == nil && len(entries) >= wantCount {
			return entries
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("read replication outbox entries: %v", lastErr)
	return nil
}

func readReplicationOutboxEntries(path string) ([]replicationOutboxEntryForTest, error) {
	dir, name := replicationOutboxPathForTest(path)
	raw, err := fs.ReadFile(os.DirFS(dir), name)
	if err != nil {
		return nil, fmt.Errorf("read replication outbox: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	entries := make([]replicationOutboxEntryForTest, 0)
	for {
		var entry replicationOutboxEntryForTest
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode replication outbox: %w", err)
		}
		entries = append(entries, entry)
	}
}

func requireReplicationAckOffset(t *testing.T, outboxPath, target string, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last replicationAckStateForTest
	var lastErr error
	for time.Now().Before(deadline) {
		state, err := readReplicationAckState(outboxPath)
		if err == nil && state.Offsets[target] == want {
			return
		}
		last = state
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replication ack offsets = %+v error = %v, want %s:%d", last, lastErr, target, want)
}

func requireEventuallyDroppedReplications(t *testing.T, server *cachetcp.Server, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := server.ReplicationStats()
		if stats.Dropped >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	last := server.ReplicationStats()
	t.Fatalf("replication dropped = %d, want >= %d", last.Dropped, want)
}

func reserveTCPAddr(t *testing.T) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve addr: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved addr: %v", err)
	}
	return listener.Addr().String()
}

func readReplicationAckState(outboxPath string) (replicationAckStateForTest, error) {
	dir, name := replicationOutboxPathForTest(outboxPath + ".acks.json")
	raw, err := fs.ReadFile(os.DirFS(dir), name)
	if err != nil {
		return replicationAckStateForTest{}, fmt.Errorf("read replication ack state: %w", err)
	}
	var state replicationAckStateForTest
	if err := json.Unmarshal(raw, &state); err != nil {
		return replicationAckStateForTest{}, fmt.Errorf("decode replication ack state: %w", err)
	}
	return state, nil
}

func replicationOutboxPathForTest(path string) (string, string) {
	clean := filepath.Clean(path)
	dir, name := filepath.Split(clean)
	if dir == "" {
		dir = "."
	}
	return dir, name
}

func writeReplicationOutboxSeed(t *testing.T, path string, sequence uint64) {
	t.Helper()
	writeReplicationOutboxEntry(t, path, replicationOutboxEntryForTest{Sequence: sequence})
}

func writeReplicationOutboxEntry(
	t *testing.T,
	path string,
	entry replicationOutboxEntryForTest,
) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("create outbox seed dir: %v", err)
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("encode outbox entry: %v", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write outbox entry: %v", err)
	}
}

func assertSetOutboxEntry(
	t *testing.T,
	entry replicationOutboxEntryForTest,
	target string,
	key cachewire.Key,
) {
	t.Helper()

	if entry.Sequence != 1 || entry.Target != target || entry.Kind != 1 || entry.Op != protocol.OpCacheSet {
		t.Fatalf("outbox entry header = %+v", entry)
	}
	request, err := cachewire.DecodeSetRequest(entry.Metadata)
	if err != nil {
		t.Fatalf("decode outbox set request: %v", err)
	}
	if !sameWireKey(request.Key, key) {
		t.Fatalf("outbox key = %+v, want %+v", request.Key, key)
	}
	if string(entry.Payload) != "payload" {
		t.Fatalf("outbox payload = %q, want payload", entry.Payload)
	}
}
