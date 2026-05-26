package tcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

type benchmarkFrameClient struct {
	conn      net.Conn
	codec     *protocol.Codec
	requestID atomic.Uint64
}

func newBenchmarkFrameClient(b *testing.B, addr string) *benchmarkFrameClient {
	b.Helper()

	client, err := newBenchmarkFrameClientForConcurrency(addr)
	if err != nil {
		b.Fatalf("create benchmark tcp client: %v", err)
	}
	return client
}

func newBenchmarkFrameClientForConcurrency(addr string) (*benchmarkFrameClient, error) {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("create benchmark tcp client: %w", err)
	}
	return &benchmarkFrameClient{
		conn:  conn,
		codec: protocol.NewCodec(),
	}, nil
}

func startServerForBenchmark(b *testing.B) (*cachetcp.Server, *benchmarkFrameClient) {
	b.Helper()

	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	server := cachetcp.NewServer(cachetcp.ServerConfig{
		Addr: "127.0.0.1:0",
	}, cache.NewService(eng))
	if err := server.Start(context.Background(), slog.New(slog.DiscardHandler)); err != nil {
		b.Fatalf("start tcp server: %v", err)
	}
	benchClient := newBenchmarkFrameClient(b, server.Addr())
	b.Cleanup(func() {
		closeBenchmarkFrameClient(b, benchClient)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			b.Fatalf("stop tcp server: %v", err)
		}
		if err := eng.Close(); err != nil {
			b.Fatalf("close cache engine: %v", err)
		}
	})

	return server, benchClient
}

func closeBenchmarkFrameClient(tb testing.TB, client *benchmarkFrameClient) {
	tb.Helper()

	if err := client.Close(); err != nil {
		tb.Errorf("close benchmark tcp client: %v", err)
	}
}

func benchmarkLimit(b *testing.B) uint64 {
	b.Helper()

	if b.N < 0 {
		b.Fatalf("benchmark N = %d, want >= 0", b.N)
	}
	limit, err := strconv.ParseUint(strconv.Itoa(b.N), 10, 64)
	if err != nil {
		b.Fatalf("parse benchmark N %d: %v", b.N, err)
	}
	return limit
}

func batchSetItems(count int) []cachewire.SetRequest {
	items := make([]cachewire.SetRequest, 0, count)
	for i := range count {
		items = append(items, cachewire.SetRequest{
			Key:       benchmarkBatchKey("batch-set", i),
			Value:     []byte("benchmark-value"),
			TTLMillis: 60_000,
		})
	}
	return items
}

func batchDeleteRequests(count int) []cachewire.DeleteRequest {
	requests := make([]cachewire.DeleteRequest, 0, count)
	for i := range count {
		requests = append(requests, cachewire.DeleteRequest{
			Key: benchmarkBatchKey("batch-set", i),
		})
	}
	return requests
}

func benchmarkBatchKey(prefix string, index int) cachewire.Key {
	return cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Entity:    "batch",
		Key:       prefix + "-" + strconv.Itoa(index),
	}
}

func benchmarkBatchKeyUint(prefix string, index uint64) cachewire.Key {
	return cachewire.Key{
		Namespace: "bench",
		Space:     "session",
		Entity:    "batch",
		Key:       prefix + "-" + strconv.FormatUint(index, 10),
	}
}

func (c *benchmarkFrameClient) Close() error {
	if c.conn == nil {
		return nil
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close benchmark frame client: %w", err)
	}
	return nil
}

func (c *benchmarkFrameClient) do(op protocol.Op, routeEpoch uint64, metadata, payload []byte) (protocol.Frame, error) {
	requestID := c.requestID.Add(1)
	frame := protocol.Frame{
		Op:         op,
		RequestID:  requestID,
		RouteEpoch: routeEpoch,
		Metadata:   metadata,
		Payload:    payload,
	}
	if err := c.codec.Encode(c.conn, frame); err != nil {
		return protocol.Frame{}, fmt.Errorf("write cache frame: %w", err)
	}
	response, err := c.codec.Decode(c.conn)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("read cache frame: %w", err)
	}
	if response.RequestID != requestID {
		return protocol.Frame{}, fmt.Errorf("cache frame request id mismatch: %d != %d", response.RequestID, requestID)
	}
	if response.Flags&protocol.FlagError != 0 {
		return response, decodeBenchmarkError(response)
	}
	return response, nil
}

func (c *benchmarkFrameClient) Set(
	_ context.Context,
	_ string,
	request cachewire.SetRequest,
) (cachewire.Record, error) {
	frame, err := c.do(protocol.OpCacheSet, request.RouteEpoch, cachewire.EncodeSetRequest(request), request.Value)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeBenchmarkRecord(frame)
}

func (c *benchmarkFrameClient) Get(
	_ context.Context,
	_ string,
	request cachewire.GetRequest,
) (cachewire.Record, error) {
	frame, err := c.do(protocol.OpCacheGet, request.RouteEpoch, cachewire.EncodeGetRequest(request), nil)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeBenchmarkRecord(frame)
}

func (c *benchmarkFrameClient) Delete(
	_ context.Context,
	_ string,
	request cachewire.DeleteRequest,
) (cachewire.DeleteResponse, error) {
	frame, err := c.do(protocol.OpCacheDelete, request.RouteEpoch, cachewire.EncodeDeleteRequest(request), nil)
	if err != nil {
		return cachewire.DeleteResponse{}, err
	}
	out, decodeErr := cachewire.DecodeDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *benchmarkFrameClient) Exists(
	_ context.Context,
	_ string,
	request cachewire.ExistsRequest,
) (cachewire.ExistsResponse, error) {
	frame, err := c.do(protocol.OpCacheExists, request.RouteEpoch, cachewire.EncodeExistsRequest(request), nil)
	if err != nil {
		return cachewire.ExistsResponse{}, err
	}
	out, decodeErr := cachewire.DecodeExistsResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache exists response: %w", decodeErr)
	}
	return out, nil
}

func (c *benchmarkFrameClient) Touch(
	_ context.Context,
	_ string,
	request cachewire.TouchRequest,
) (cachewire.TouchResponse, error) {
	frame, err := c.do(protocol.OpCacheTouch, request.RouteEpoch, cachewire.EncodeTouchRequest(request), nil)
	if err != nil {
		return cachewire.TouchResponse{}, err
	}
	out, decodeErr := cachewire.DecodeTouchResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *benchmarkFrameClient) BatchSet(
	_ context.Context,
	_ string,
	request cachewire.BatchSetRequest,
) (cachewire.BatchSetResponse, error) {
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("encode cache batch set request: %w", err)
	}
	frame, err := c.do(protocol.OpCacheBatchSet, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.BatchSetResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchSetResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch set response: %w", decodeErr)
	}
	return out, nil
}

func (c *benchmarkFrameClient) BatchDelete(
	_ context.Context,
	_ string,
	request cachewire.BatchDeleteRequest,
) (cachewire.BatchDeleteResponse, error) {
	frame, err := c.do(protocol.OpCacheBatchDelete, request.RouteEpoch, cachewire.EncodeBatchDeleteRequest(request), nil)
	if err != nil {
		return cachewire.BatchDeleteResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch delete response: %w", decodeErr)
	}
	return out, nil
}

func decodeBenchmarkRecord(frame protocol.Frame) (cachewire.Record, error) {
	record, err := cachewire.DecodeRecord(frame.Metadata)
	if err != nil {
		return record, fmt.Errorf("decode cache record response: %w", err)
	}
	if len(frame.Payload) > 0 {
		record.Value = append(record.Value[:0], frame.Payload...)
	}
	return record, nil
}

func decodeBenchmarkError(frame protocol.Frame) error {
	var errResp cachewire.Error
	if err := json.Unmarshal(frame.Metadata, &errResp); err != nil {
		return fmt.Errorf("decode cache error: %w", err)
	}
	return errResp
}
