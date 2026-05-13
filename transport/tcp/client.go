// Package tcp implements the framed cache transport.
package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/nespa/cacheapi"
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

func (c *Client) Set(ctx context.Context, addr string, body cacheapi.SetBody) (cacheapi.RecordBody, error) {
	payload := []byte(body.Value)
	body.Value = ""
	frame, err := c.do(ctx, addr, protocol.OpCacheSet, body, payload)
	if err != nil {
		return cacheapi.RecordBody{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Get(ctx context.Context, addr string, input cacheapi.GetInput) (cacheapi.RecordBody, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheGet, input, nil)
	if err != nil {
		return cacheapi.RecordBody{}, err
	}
	return decodeRecord(frame)
}

func (c *Client) Delete(ctx context.Context, addr string, input cacheapi.DeleteInput) (cacheapi.DeleteBody, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheDelete, input, nil)
	if err != nil {
		return cacheapi.DeleteBody{}, err
	}
	var out cacheapi.DeleteBody
	if err := json.Unmarshal(frame.Metadata, &out); err != nil {
		return out, fmt.Errorf("decode cache delete response: %w", err)
	}
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
	if encodeErr := c.codec.Encode(conn, protocol.Frame{Op: op, RequestID: requestID, Metadata: raw, Payload: payload}); encodeErr != nil {
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

func (c *Client) dial(ctx context.Context, addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: c.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", normalizeAddr(addr))
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

func decodeRecord(frame protocol.Frame) (cacheapi.RecordBody, error) {
	var out cacheapi.RecordBody
	if err := json.Unmarshal(frame.Metadata, &out); err != nil {
		return out, fmt.Errorf("decode cache record response: %w", err)
	}
	if len(frame.Payload) > 0 {
		out.Value = string(frame.Payload)
	}
	return out, nil
}

func decodeError(frame protocol.Frame) error {
	var body cacheapi.ErrorBody
	if err := json.Unmarshal(frame.Metadata, &body); err != nil {
		return errors.New("cache tcp error")
	}
	if body.Message == "" {
		return fmt.Errorf("cache tcp error: %s", body.Code)
	}
	return fmt.Errorf("cache tcp error: %s", body.Message)
}

func closeConn(conn net.Conn) {
	if err := conn.Close(); err != nil {
		return
	}
}
