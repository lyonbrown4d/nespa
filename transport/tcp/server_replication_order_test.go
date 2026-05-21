package tcp_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerSerializesReplicationPerTarget(t *testing.T) {
	replica := startBlockingSetReplica(t)
	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "ordered"}
	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr: "127.0.0.1:0",
		ReplicaTargets: func(in cachewire.Key) []string {
			if sameWireKey(in, key) {
				return []string{replica.Addr()}
			}
			return nil
		},
	})

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{Key: key, Value: []byte("first")}); err != nil {
		t.Fatalf("set first source: %v", err)
	}
	waitBlockingReplicaSignal(t, replica.firstReceived)

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{Key: key, Value: []byte("second")}); err != nil {
		t.Fatalf("set second source: %v", err)
	}
	select {
	case <-replica.secondReceived:
		t.Fatal("second replication reached target before first replication completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(replica.releaseFirst)
	waitBlockingReplicaSignal(t, replica.secondReceived)
}

func TestClientServerRetriesReplicationAfterTransientFailure(t *testing.T) {
	replica := startTransientFailingSetReplica(t)
	client := cachetcp.NewClient()
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "retry"}
	source := startCacheServer(t, cachetcp.ServerConfig{
		Addr: "127.0.0.1:0",
		ReplicaTargets: func(in cachewire.Key) []string {
			if sameWireKey(in, key) {
				return []string{replica.Addr()}
			}
			return nil
		},
	})

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{Key: key, Value: []byte("payload")}); err != nil {
		t.Fatalf("set source: %v", err)
	}

	waitBlockingReplicaSignal(t, replica.firstFailed)
	waitBlockingReplicaSignal(t, replica.retrySucceeded)
	requireEventuallyReplicationRetrySequence(t, source, 1)
}

type blockingSetReplica struct {
	listener       net.Listener
	firstReceived  chan struct{}
	secondReceived chan struct{}
	releaseFirst   chan struct{}

	mu    sync.Mutex
	count int
}

type transientFailingSetReplica struct {
	listener       net.Listener
	firstFailed    chan struct{}
	retrySucceeded chan struct{}

	mu    sync.Mutex
	count int
}

func startBlockingSetReplica(t *testing.T) *blockingSetReplica {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen blocking replica: %v", err)
	}
	replica := &blockingSetReplica{
		listener:       listener,
		firstReceived:  make(chan struct{}),
		secondReceived: make(chan struct{}),
		releaseFirst:   make(chan struct{}),
	}
	go replica.accept()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			return
		}
	})
	return replica
}

func startTransientFailingSetReplica(t *testing.T) *transientFailingSetReplica {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen transient failing replica: %v", err)
	}
	replica := &transientFailingSetReplica{
		listener:       listener,
		firstFailed:    make(chan struct{}),
		retrySucceeded: make(chan struct{}),
	}
	go replica.accept()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			return
		}
	})
	return replica
}

func (r *blockingSetReplica) Addr() string {
	return r.listener.Addr().String()
}

func (r *transientFailingSetReplica) Addr() string {
	return r.listener.Addr().String()
}

func (r *blockingSetReplica) accept() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			return
		}
		go r.handleConn(conn)
	}
}

func (r *transientFailingSetReplica) accept() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			return
		}
		go r.handleConn(conn)
	}
}

func (r *blockingSetReplica) handleConn(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			return
		}
	}()

	codec := protocol.NewCodec()
	frame, err := codec.Decode(conn)
	if err != nil {
		return
	}
	index := r.nextRequestIndex()
	switch index {
	case 1:
		close(r.firstReceived)
		<-r.releaseFirst
	case 2:
		close(r.secondReceived)
	}
	if err := codec.Encode(conn, blockingReplicaSetResponse(frame, index)); err != nil {
		return
	}
}

func (r *transientFailingSetReplica) handleConn(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			return
		}
	}()

	codec := protocol.NewCodec()
	frame, err := codec.Decode(conn)
	if err != nil {
		return
	}
	index := r.nextRequestIndex()
	if index == 1 {
		close(r.firstFailed)
		return
	}
	close(r.retrySucceeded)
	if err := codec.Encode(conn, blockingReplicaSetResponse(frame, index)); err != nil {
		return
	}
}

func (r *blockingSetReplica) nextRequestIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return r.count
}

func (r *transientFailingSetReplica) nextRequestIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return r.count
}

func blockingReplicaSetResponse(frame protocol.Frame, index int) protocol.Frame {
	version := uint64(1)
	if index == 2 {
		version = 2
	}
	return protocol.Frame{
		Flags:     protocol.FlagResponse,
		Op:        frame.Op,
		RequestID: frame.RequestID,
		Metadata: cachewire.EncodeRecord(cachewire.Record{
			Found:   true,
			Version: version,
		}),
	}
}

func waitBlockingReplicaSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocking replica signal")
	}
}

func requireEventuallyReplicationRetrySequence(t *testing.T, server *cachetcp.Server, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last cachetcp.ReplicationStats
	for time.Now().Before(deadline) {
		last = server.ReplicationStats()
		if replicationRetryStatsReachedSequence(last, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replication retry stats = %+v, want retried sequence %d", last, want)
}

func replicationRetryStatsReachedSequence(stats cachetcp.ReplicationStats, want uint64) bool {
	return stats.Enqueued == want &&
		stats.Attempts == 2 &&
		stats.Successes == 1 &&
		stats.Failures == 1 &&
		stats.LastQueuedSequence == want &&
		stats.LastAttemptSequence == want &&
		stats.LastSuccessSequence == want &&
		stats.LastFailureSequence == want
}
