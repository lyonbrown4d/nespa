// Package tcp implements the framed cache transport.
package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	clienttcp "github.com/arcgolabs/clientx/tcp"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

type Client struct {
	codec   *protocol.Codec
	timeout time.Duration
	nextID  atomic.Uint64
}

func NewClient() *Client {
	return &Client{
		codec:   protocol.NewCodec(),
		timeout: 5 * time.Second,
	}
}

func (c *Client) Set(ctx context.Context, addr string, request cachewire.SetRequest) (cachewire.Record, error) {
	payload := request.Value
	metadata := cachewire.EncodeSetRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheSet, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Get(ctx context.Context, addr string, request cachewire.GetRequest) (cachewire.Record, error) {
	metadata := cachewire.EncodeGetRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheGet, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Delete(ctx context.Context, addr string, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	metadata := cachewire.EncodeDeleteRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheDelete, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.DeleteResponse{}, err
	}
	out, decodeErr := cachewire.DecodeDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) Exists(ctx context.Context, addr string, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	metadata := cachewire.EncodeExistsRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheExists, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.ExistsResponse{}, err
	}
	out, decodeErr := cachewire.DecodeExistsResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache exists response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) Touch(ctx context.Context, addr string, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	metadata := cachewire.EncodeTouchRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheTouch, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.TouchResponse{}, err
	}
	out, decodeErr := cachewire.DecodeTouchResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) Adjust(ctx context.Context, addr string, request cachewire.AdjustRequest) (cachewire.Record, error) {
	metadata := cachewire.EncodeAdjustRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpCacheAdjust, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Primitive(
	ctx context.Context,
	addr string,
	request cachewire.PrimitiveRequest,
) (cachewire.PrimitiveResult, error) {
	metadata, payload, err := cachewire.EncodePrimitiveRequest(request)
	if err != nil {
		return cachewire.PrimitiveResult{}, fmt.Errorf("encode cache primitive request: %w", err)
	}
	frame, err := c.do(ctx, addr, protocol.OpCachePrimitive, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.PrimitiveResult{}, err
	}
	out, decodeErr := cachewire.DecodePrimitiveResponse(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache primitive response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) do(ctx context.Context, addr string, op protocol.Op, routeEpoch uint64, metadata, payload []byte) (protocol.Frame, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return protocol.Frame{}, err
	}
	defer closeConn(conn)

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
		return protocol.Frame{}, decodeError(frame)
	}
	return frame, nil
}

func (c *Client) dial(ctx context.Context, addr string) (io.ReadWriteCloser, error) {
	client, err := clienttcp.New(clienttcp.Config{
		Address:      normalizeAddr(addr),
		DialTimeout:  c.timeout,
		ReadTimeout:  c.timeout,
		WriteTimeout: c.timeout,
		KeepAlive:    30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create cache tcp client: %w", err)
	}
	conn, err := client.Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial cache tcp server: %w", err)
	}
	return conn, nil
}

func normalizeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if parsed, err := url.Parse(addr); err == nil && parsed.Host != "" {
		return parsed.Host
	}
	return addr
}

func decodeRecord(frame protocol.Frame) (cachewire.Record, error) {
	out, err := cachewire.DecodeRecord(frame.Metadata)
	if err != nil {
		return out, fmt.Errorf("decode cache record response: %w", err)
	}
	if len(frame.Payload) > 0 {
		out.Value = append(out.Value[:0], frame.Payload...)
	}
	return out, nil
}

func decodeError(frame protocol.Frame) error {
	var body cachewire.Error
	if err := json.Unmarshal(frame.Metadata, &body); err != nil {
		return errors.New("cache tcp error")
	}
	return body
}

func closeConn(conn interface{ Close() error }) {
	if err := conn.Close(); err != nil {
		return
	}
}
