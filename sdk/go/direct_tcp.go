package nespa

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	clienttcp "github.com/arcgolabs/clientx/tcp"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

const directTCPTimeout = 5 * time.Second

type directTCPClient struct {
	codec  *protocol.Codec
	tcp    clienttcp.Client
	nextID atomic.Uint64
}

func newDirectTCPClient(addr string) (*directTCPClient, error) {
	client, err := clienttcp.New(clienttcp.Config{
		Address:      normalizeDirectTCPAddr(addr),
		DialTimeout:  directTCPTimeout,
		ReadTimeout:  directTCPTimeout,
		WriteTimeout: directTCPTimeout,
		KeepAlive:    30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create clientx tcp client: %w", err)
	}
	return &directTCPClient{
		codec: protocol.NewCodec(),
		tcp:   client,
	}, nil
}

func (c *directTCPClient) Set(ctx context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
	frame, err := c.do(ctx, protocol.OpCacheSet, request.RouteEpoch, cachewire.EncodeSetRequest(request), request.Value)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("set cache record: %w", err)
	}
	return decodeDirectRecord(frame)
}

func (c *directTCPClient) Get(ctx context.Context, request cachewire.GetRequest) (cachewire.Record, error) {
	frame, err := c.do(ctx, protocol.OpCacheGet, request.RouteEpoch, cachewire.EncodeGetRequest(request), nil)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("get cache record: %w", err)
	}
	return decodeDirectRecord(frame)
}

func (c *directTCPClient) Delete(ctx context.Context, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheDelete, request.RouteEpoch, cachewire.EncodeDeleteRequest(request), nil)
	if err != nil {
		return cachewire.DeleteResponse{}, fmt.Errorf("delete cache record: %w", err)
	}
	out, decodeErr := cachewire.DecodeDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) Exists(ctx context.Context, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheExists, request.RouteEpoch, cachewire.EncodeExistsRequest(request), nil)
	if err != nil {
		return cachewire.ExistsResponse{}, fmt.Errorf("check cache record: %w", err)
	}
	out, decodeErr := cachewire.DecodeExistsResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache exists response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) Touch(ctx context.Context, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheTouch, request.RouteEpoch, cachewire.EncodeTouchRequest(request), nil)
	if err != nil {
		return cachewire.TouchResponse{}, fmt.Errorf("touch cache record: %w", err)
	}
	out, decodeErr := cachewire.DecodeTouchResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) Adjust(ctx context.Context, request cachewire.AdjustRequest) (cachewire.Record, error) {
	frame, err := c.do(ctx, protocol.OpCacheAdjust, request.RouteEpoch, cachewire.EncodeAdjustRequest(request), nil)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("adjust cache record: %w", err)
	}
	return decodeDirectRecord(frame)
}

func (c *directTCPClient) Primitive(
	ctx context.Context,
	request cachewire.PrimitiveRequest,
) (cachewire.PrimitiveResult, error) {
	metadata, payload, err := cachewire.EncodePrimitiveRequest(request)
	if err != nil {
		return cachewire.PrimitiveResult{}, fmt.Errorf("encode cache primitive request: %w", err)
	}
	frame, err := c.do(ctx, protocol.OpCachePrimitive, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.PrimitiveResult{}, fmt.Errorf("execute cache primitive: %w", err)
	}
	return decodeDirectPrimitiveResult(frame)
}

func (c *directTCPClient) BatchSet(ctx context.Context, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("encode cache batch set request: %w", err)
	}
	frame, err := c.do(ctx, protocol.OpCacheBatchSet, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("batch set cache records: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchSetResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch set response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) BatchGet(ctx context.Context, request cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheBatchGet, request.RouteEpoch, cachewire.EncodeBatchGetRequest(request), nil)
	if err != nil {
		return cachewire.BatchGetResponse{}, fmt.Errorf("batch get cache records: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchGetResponse(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch get response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) BatchDelete(
	ctx context.Context,
	request cachewire.BatchDeleteRequest,
) (cachewire.BatchDeleteResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheBatchDelete, request.RouteEpoch,
		cachewire.EncodeBatchDeleteRequest(request), nil)
	if err != nil {
		return cachewire.BatchDeleteResponse{}, fmt.Errorf("batch delete cache records: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) BatchExists(
	ctx context.Context,
	request cachewire.BatchExistsRequest,
) (cachewire.BatchExistsResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheBatchExists, request.RouteEpoch,
		cachewire.EncodeBatchExistsRequest(request), nil)
	if err != nil {
		return cachewire.BatchExistsResponse{}, fmt.Errorf("batch exists cache records: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchExistsResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch exists response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) BatchTouch(
	ctx context.Context,
	request cachewire.BatchTouchRequest,
) (cachewire.BatchTouchResponse, error) {
	frame, err := c.do(ctx, protocol.OpCacheBatchTouch, request.RouteEpoch,
		cachewire.EncodeBatchTouchRequest(request), nil)
	if err != nil {
		return cachewire.BatchTouchResponse{}, fmt.Errorf("batch touch cache records: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchTouchResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) BatchPrimitive(
	ctx context.Context,
	request cachewire.BatchPrimitiveRequest,
) (cachewire.BatchPrimitiveResponse, error) {
	metadata, payload, err := cachewire.EncodeBatchPrimitiveRequest(request)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{}, fmt.Errorf("encode cache batch primitive request: %w", err)
	}
	frame, err := c.do(ctx, protocol.OpCacheBatchPrimitive, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{}, fmt.Errorf("batch execute cache primitives: %w", err)
	}
	out, decodeErr := cachewire.DecodeBatchPrimitiveResponse(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch primitive response: %w", decodeErr)
	}
	return out, nil
}

func (c *directTCPClient) do(ctx context.Context, op protocol.Op, routeEpoch uint64, metadata, payload []byte) (protocol.Frame, error) {
	conn, err := c.tcp.Dial(ctx)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("dial cache tcp server: %w", err)
	}
	defer closeDirectConn(conn)

	requestID := c.nextID.Add(1)
	if encodeErr := c.codec.Encode(conn, protocol.Frame{
		Op:         op,
		RequestID:  requestID,
		RouteEpoch: routeEpoch,
		Metadata:   metadata,
		Payload:    payload,
	}); encodeErr != nil {
		return protocol.Frame{}, fmt.Errorf("write cache frame: %w", encodeErr)
	}

	frame, err := c.codec.Decode(conn)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("read cache frame: %w", err)
	}
	if frame.RequestID != requestID {
		return protocol.Frame{}, fmt.Errorf("cache frame request id mismatch: %d != %d", frame.RequestID, requestID)
	}
	if frame.Flags&protocol.FlagError != 0 {
		return protocol.Frame{}, decodeDirectError(frame)
	}
	return frame, nil
}

func normalizeDirectTCPAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if parsed, err := url.Parse(addr); err == nil && parsed.Host != "" {
		return parsed.Host
	}
	return addr
}

func decodeDirectRecord(frame protocol.Frame) (cachewire.Record, error) {
	out, err := cachewire.DecodeRecord(frame.Metadata)
	if err != nil {
		return out, fmt.Errorf("decode cache record response: %w", err)
	}
	if len(frame.Payload) > 0 {
		out.Value = append(out.Value[:0], frame.Payload...)
	}
	return out, nil
}

func decodeDirectPrimitiveResult(frame protocol.Frame) (cachewire.PrimitiveResult, error) {
	out, err := cachewire.DecodePrimitiveResponse(frame.Metadata, frame.Payload)
	if err != nil {
		return out, fmt.Errorf("decode cache primitive response: %w", err)
	}
	return out, nil
}

func decodeDirectError(frame protocol.Frame) error {
	var body cachewire.Error
	if err := json.Unmarshal(frame.Metadata, &body); err != nil {
		return errors.New("cache tcp error")
	}
	return body
}

func closeDirectConn(conn interface{ Close() error }) {
	if err := conn.Close(); err != nil {
		return
	}
}
