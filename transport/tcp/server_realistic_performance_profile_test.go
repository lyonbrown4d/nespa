package tcp_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

const profileConcurrency = 16

func TestProfileEndpointSetGetTraffic(t *testing.T) {
	addr, stopServer := resolveProfileTarget(t)
	defer stopServer()

	const operations = 20_000
	payloadSizes := []int{64, 1024, 16 * 1024}

	for _, payloadSize := range payloadSizes {
		t.Run(fmt.Sprintf("payload=%d", payloadSize), func(t *testing.T) {
			result := runProfileScenario(
				t,
				addr,
				operations,
				func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
					key := benchmarkBatchKeyUint("profile", opIndex)
					payload := bytes.Repeat([]byte("x"), payloadSize)
					if _, err := client.Set(ctx, addr, cachewire.SetRequest{
						Key:       key,
						Value:     payload,
						TTLMillis: 60_000,
					}); err != nil {
						return err
					}
					if _, err := client.Get(ctx, addr, cachewire.GetRequest{
						Key: key,
					}); err != nil {
						return err
					}
					return nil
				},
				"set+get",
				payloadSize,
			)
			t.Logf("endpoint set/get payload=%dB: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
				payloadSize, result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
		})
	}
}

func TestProfileReplicationThroughput(t *testing.T) {
	replica, _, stopReplica := startProfileServerWithService(t, cachetcp.ServerConfig{})
	defer stopReplica()

	primary, _, stopPrimary := startProfileServerWithService(t, cachetcp.ServerConfig{
		ReplicaTargets: func(key cachewire.Key) []string {
			return []string{replica.Addr()}
		},
		ReplicationOutboxPath: filepath.Join(t.TempDir(), "replication.outbox"),
		ReplicationQueueSize:  20_000,
	})
	defer stopPrimary()

	const operations = 2_000
	const payloadSize = 1024

	result := runProfileScenario(
		t,
		primary.Addr(),
		operations,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			return runSet(ctx, client, primary.Addr(), benchmarkBatchKeyUint("replication", opIndex), payloadSize)
		},
		"set-only",
		payloadSize,
	)

	if err := waitReplicationCatchup(primary, uint64(operations), 20*time.Second); err != nil {
		t.Logf("replication catchup: %v", err)
	}

	t.Logf("replication set: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
	t.Logf("replication stats=%+v", primary.ReplicationStats())
}

func TestProfilePersistenceSnapshotRestore(t *testing.T) {
	server, service, stopServer := startProfileServerWithService(t, cachetcp.ServerConfig{})
	defer stopServer()

	const writes = 8_000
	const reads = 5_000
	const payloadSize = 1024

	seedAddr := server.Addr()

	seedResult := runProfileScenario(
		t,
		seedAddr,
		writes,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			return runSet(ctx, client, seedAddr, benchmarkBatchKeyUint("persist", opIndex), payloadSize)
		},
		"snapshot seed set",
		payloadSize,
	)
	_ = seedResult

	exportStart := time.Now()
	snapshot, err := service.Export(context.Background(), cache.RangeOptions{
		Namespace:  "bench",
		Space:      "session",
		VSlotStart: 0,
		VSlotEnd:   ^uint32(0),
	})
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}
	exportDuration := time.Since(exportStart)

	restoreStart := time.Now()
	restoredServer, stopRestored := restoreProfileServerFromSnapshot(t, cachetcp.ServerConfig{}, snapshot)
	defer stopRestored()
	restoreDuration := time.Since(restoreStart)

	restoreResult := runProfileScenario(
		t,
		restoredServer.Addr(),
		reads,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			_, err := client.Get(ctx, restoredServer.Addr(), cachewire.GetRequest{
				Key: benchmarkBatchKeyUint("persist", opIndex),
			})
			return err
		},
		"snapshot restore read",
		payloadSize,
	)

	t.Logf("persistence seed set: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		seedResult.qps, seedResult.failureRate*100, seedResult.p50, seedResult.p95, seedResult.p99)
	t.Logf("persistence restore read: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		restoreResult.qps, restoreResult.failureRate*100, restoreResult.p50, restoreResult.p95, restoreResult.p99)
	t.Logf("persistence durations: snapshot=%s restore=%s", exportDuration, restoreDuration)
}

func TestProfileRoutingEpochMix(t *testing.T) {
	currentEpoch := atomic.Uint64{}
	currentEpoch.Store(2)

	server, _, stopServer := startProfileServerWithService(t, cachetcp.ServerConfig{
		CurrentRouteEpoch: currentEpoch.Load,
	})
	defer stopServer()

	const operations = 12_000
	const payloadSize = 256

	result := runProfileScenario(
		t,
		server.Addr(),
		operations,
		func(ctx context.Context, client *benchmarkFrameClient, opIndex uint64) error {
			key := benchmarkBatchKeyUint("route", opIndex%4096)
			request := cachewire.SetRequest{
				Key:       key,
				Value:     bytes.Repeat([]byte("x"), payloadSize),
				TTLMillis: 60_000,
			}
			if opIndex%2 == 0 {
				request.RouteEpoch = 1 // stale for this setup
			} else {
				request.RouteEpoch = currentEpoch.Load()
			}
			_, err := client.Set(ctx, server.Addr(), request)
			return err
		},
		"route mixed epoch",
		payloadSize,
	)

	t.Logf("routing mixed epoch: qps=%.1f fail=%.4f%% p50=%s p95=%s p99=%s",
		result.qps, result.failureRate*100, result.p50, result.p95, result.p99)
}
