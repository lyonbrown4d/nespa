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
	request.Value = nil
	frame, err := c.do(ctx, addr, protocol.OpCacheSet, request, payload)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Get(ctx context.Context, addr string, request cachewire.GetRequest) (cachewire.Record, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheGet, request, nil)
	if err != nil {
		return cachewire.Record{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Delete(ctx context.Context, addr string, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheDelete, request, nil)
	if err != nil {
		return cachewire.DeleteResponse{}, err
	}
	var out cachewire.DeleteResponse
	if decodeErr := json.Unmarshal(frame.Metadata, &out); decodeErr != nil {
		return out, fmt.Errorf("decode cache delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) Exists(ctx context.Context, addr string, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheExists, request, nil)
	if err != nil {
		return cachewire.ExistsResponse{}, err
	}
	var out cachewire.ExistsResponse
	if decodeErr := json.Unmarshal(frame.Metadata, &out); decodeErr != nil {
		return out, fmt.Errorf("decode cache exists response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) Touch(ctx context.Context, addr string, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheTouch, request, nil)
	if err != nil {
		return cachewire.TouchResponse{}, err
	}
	var out cachewire.TouchResponse
	if decodeErr := json.Unmarshal(frame.Metadata, &out); decodeErr != nil {
		return out, fmt.Errorf("decode cache touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchSet(ctx context.Context, addr string, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	metadata, payload, err := cachewire.PackBatchSet(request)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("pack cache batch set request: %w", err)
	}
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchSet, metadata, payload)
	if err != nil {
		return cachewire.BatchSetResponse{}, err
	}
	var out cachewire.BatchSetResponse
	if decodeErr := json.Unmarshal(frame.Metadata, &out); decodeErr != nil {
		return out, fmt.Errorf("decode cache batch set response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchGet(ctx context.Context, addr string, request cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchGet, request, nil)
	if err != nil {
		return cachewire.BatchGetResponse{}, err
	}
	var out cachewire.BatchGetResponse
	if decodeErr := json.Unmarshal(frame.Metadata, &out); decodeErr != nil {
		return out, fmt.Errorf("decode cache batch get response: %w", decodeErr)
	}
	records, err := cachewire.UnpackRecords(out, frame.Payload)
	if err != nil {
		return out, fmt.Errorf("unpack cache batch get response: %w", err)
	}
	out.Records = records
	return out, nil
}

func (c *Client) do(ctx context.Context, addr string, op protocol.Op, metadata any, payload []byte) (protocol.Frame, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return protocol.Frame{}, err
	}
	defer closeConn(conn)

	raw, err := json.Marshal(metadata)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("encode cache frame metadata: %w", err)
	}

	requestID := c.nextID.Add(1)
	if encodeErr := c.codec.Encode(conn, protocol.Frame{
		Op:         op,
		RequestID:  requestID,
		RouteEpoch: routeEpoch(metadata),
		Metadata:   raw,
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

func routeEpoch(metadata any) uint64 {
	switch item := metadata.(type) {
	case cachewire.SetRequest:
		return item.RouteEpoch
	case cachewire.GetRequest:
		return item.RouteEpoch
	case cachewire.DeleteRequest:
		return item.RouteEpoch
	case cachewire.ExistsRequest:
		return item.RouteEpoch
	case cachewire.TouchRequest:
		return item.RouteEpoch
	case cachewire.BatchSetRequest:
		return item.RouteEpoch
	case cachewire.BatchGetRequest:
		return item.RouteEpoch
	default:
		return 0
	}
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
	var out cachewire.Record
	if err := json.Unmarshal(frame.Metadata, &out); err != nil {
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
