// Package client exposes the public Nespa client SDK.
package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
	"github.com/samber/oops"
)

var ErrInvalidConfig = oops.Code("invalid_config").In("client").New("client: invalid config")

type Config struct {
	Addr string
}

type TCPClient struct {
	addr      string
	transport *cachetcp.Client
}

func NewTCP(cfg Config) (*TCPClient, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil, oops.Code("invalid_config").
			In("client").
			With("addr", cfg.Addr).
			Wrap(ErrInvalidConfig)
	}
	return &TCPClient{
		addr:      addr,
		transport: cachetcp.NewClient(),
	}, nil
}

func (c *TCPClient) Set(ctx context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
	record, err := c.transport.Set(ctx, c.addr, request)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("set cache record: %w", err)
	}
	return record, nil
}

func (c *TCPClient) Get(ctx context.Context, request cachewire.GetRequest) (cachewire.Record, error) {
	record, err := c.transport.Get(ctx, c.addr, request)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("get cache record: %w", err)
	}
	return record, nil
}

func (c *TCPClient) Delete(ctx context.Context, request cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	response, err := c.transport.Delete(ctx, c.addr, request)
	if err != nil {
		return cachewire.DeleteResponse{}, fmt.Errorf("delete cache record: %w", err)
	}
	return response, nil
}

func (c *TCPClient) Exists(ctx context.Context, request cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	response, err := c.transport.Exists(ctx, c.addr, request)
	if err != nil {
		return cachewire.ExistsResponse{}, fmt.Errorf("check cache record: %w", err)
	}
	return response, nil
}

func (c *TCPClient) Touch(ctx context.Context, request cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	response, err := c.transport.Touch(ctx, c.addr, request)
	if err != nil {
		return cachewire.TouchResponse{}, fmt.Errorf("touch cache record: %w", err)
	}
	return response, nil
}

func (c *TCPClient) Adjust(ctx context.Context, request cachewire.AdjustRequest) (cachewire.Record, error) {
	record, err := c.transport.Adjust(ctx, c.addr, request)
	if err != nil {
		return cachewire.Record{}, fmt.Errorf("adjust cache record: %w", err)
	}
	return record, nil
}

func (c *TCPClient) Primitive(
	ctx context.Context,
	request cachewire.PrimitiveRequest,
) (cachewire.PrimitiveResult, error) {
	result, err := c.transport.Primitive(ctx, c.addr, request)
	if err != nil {
		return cachewire.PrimitiveResult{}, fmt.Errorf("execute cache primitive: %w", err)
	}
	return result, nil
}

func (c *TCPClient) BatchSet(ctx context.Context, request cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	response, err := c.transport.BatchSet(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("batch set cache records: %w", err)
	}
	return response, nil
}

func (c *TCPClient) BatchPrimitive(
	ctx context.Context,
	request cachewire.BatchPrimitiveRequest,
) (cachewire.BatchPrimitiveResponse, error) {
	response, err := c.transport.BatchPrimitive(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{}, fmt.Errorf("batch execute cache primitives: %w", err)
	}
	return response, nil
}

func (c *TCPClient) BatchGet(ctx context.Context, request cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error) {
	response, err := c.transport.BatchGet(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchGetResponse{}, fmt.Errorf("batch get cache records: %w", err)
	}
	return response, nil
}

func (c *TCPClient) BatchDelete(
	ctx context.Context,
	request cachewire.BatchDeleteRequest,
) (cachewire.BatchDeleteResponse, error) {
	response, err := c.transport.BatchDelete(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchDeleteResponse{}, fmt.Errorf("batch delete cache records: %w", err)
	}
	return response, nil
}

func (c *TCPClient) BatchExists(
	ctx context.Context,
	request cachewire.BatchExistsRequest,
) (cachewire.BatchExistsResponse, error) {
	response, err := c.transport.BatchExists(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchExistsResponse{}, fmt.Errorf("batch check cache records: %w", err)
	}
	return response, nil
}

func (c *TCPClient) BatchTouch(
	ctx context.Context,
	request cachewire.BatchTouchRequest,
) (cachewire.BatchTouchResponse, error) {
	response, err := c.transport.BatchTouch(ctx, c.addr, request)
	if err != nil {
		return cachewire.BatchTouchResponse{}, fmt.Errorf("batch touch cache records: %w", err)
	}
	return response, nil
}
