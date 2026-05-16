package tcp

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (c *Client) BatchTouch(
	ctx context.Context,
	addr string,
	request cachewire.BatchTouchRequest,
) (cachewire.BatchTouchResponse, error) {
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchTouch, request.RouteEpoch,
		cachewire.EncodeBatchTouchRequest(request), nil)
	if err != nil {
		return cachewire.BatchTouchResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchTouchResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch touch response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) BatchPrimitive(
	ctx context.Context,
	addr string,
	request cachewire.BatchPrimitiveRequest,
) (cachewire.BatchPrimitiveResponse, error) {
	metadata, payload, err := cachewire.EncodeBatchPrimitiveRequest(request)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{}, fmt.Errorf("encode cache batch primitive request: %w", err)
	}
	frame, err := c.do(ctx, addr, protocol.OpCacheBatchPrimitive, request.RouteEpoch, metadata, payload)
	if err != nil {
		return cachewire.BatchPrimitiveResponse{}, err
	}
	out, decodeErr := cachewire.DecodeBatchPrimitiveResponse(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache batch primitive response: %w", decodeErr)
	}
	return out, nil
}
