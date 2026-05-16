package tcp

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (c *Client) BatchSet(
	ctx context.Context,
	addr string,
	request cachewire.BatchSetRequest,
) (cachewire.BatchSetResponse, error) {
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		return cachewire.BatchSetResponse{}, fmt.Errorf("encode cache batch set request: %w", err)
	}
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchSet, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.BatchSetResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchSetResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch set response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchGet(
	ctx context.Context,
	addr string,
	request cachewire.BatchGetRequest,
) (cachewire.BatchGetResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchGet, request.RouteEpoch,
		cachewire.EncodeBatchGetRequest(request), nil)
	if err != nil {
		return cachewire.BatchGetResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchGetResponse(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch get response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchDelete(
	ctx context.Context,
	addr string,
	request cachewire.BatchDeleteRequest,
) (cachewire.BatchDeleteResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchDelete, request.RouteEpoch,
		cachewire.EncodeBatchDeleteRequest(request), nil)
	if err != nil {
		return cachewire.BatchDeleteResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchDeleteResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch delete response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchExists(
	ctx context.Context,
	addr string,
	request cachewire.BatchExistsRequest,
) (cachewire.BatchExistsResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchExists, request.RouteEpoch,
		cachewire.EncodeBatchExistsRequest(request), nil)
	if err != nil {
		return cachewire.BatchExistsResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchExistsResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch exists response: %w", decodeErr)
	}
	return out, nil
}
