package tcp

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (c *Client) ExportRange(
	ctx context.Context,
	addr string,
	request cachewire.MigrationRangeRequest,
) (cachewire.MigrationSnapshot, error) {
	metadata := cachewire.EncodeMigrationRangeRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpNodeExportRange, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.MigrationSnapshot{}, err
	}
	out, decodeErr := cachewire.DecodeMigrationSnapshot(frame.Metadata, frame.Payload)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache migration snapshot: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) ImportSnapshot(
	ctx context.Context,
	addr string,
	snapshot cachewire.MigrationSnapshot,
) (cachewire.MigrationImportResponse, error) {
	metadata, payload, err := cachewire.EncodeMigrationSnapshot(snapshot)
	if err != nil {
		return cachewire.MigrationImportResponse{}, fmt.Errorf("encode cache migration snapshot: %w", err)
	}
	frame, err := c.do(ctx, addr, protocol.OpNodeImportSnapshot, 0, metadata, payload)
	if err != nil {
		return cachewire.MigrationImportResponse{}, err
	}
	out, decodeErr := cachewire.DecodeMigrationImportResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache migration import response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) DeleteRange(
	ctx context.Context,
	addr string,
	request cachewire.MigrationRangeRequest,
) (cachewire.MigrationDeleteRangeResponse, error) {
	metadata := cachewire.EncodeMigrationRangeRequest(request)
	frame, err := c.do(ctx, addr, protocol.OpNodeDeleteRange, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.MigrationDeleteRangeResponse{}, err
	}
	out, decodeErr := cachewire.DecodeMigrationDeleteRangeResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache migration delete range response: %w", decodeErr)
	}
	return out, nil
}

func (c *Client) FenceRange(
	ctx context.Context,
	addr string,
	request cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	return c.rangeFenceCommand(ctx, addr, protocol.OpNodeFenceRange, request)
}

func (c *Client) UnfenceRange(
	ctx context.Context,
	addr string,
	request cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	return c.rangeFenceCommand(ctx, addr, protocol.OpNodeUnfenceRange, request)
}

func (c *Client) rangeFenceCommand(
	ctx context.Context,
	addr string,
	op protocol.Op,
	request cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	metadata := cachewire.EncodeMigrationRangeRequest(request)
	frame, err := c.do(ctx, addr, op, request.RouteEpoch, metadata, nil)
	if err != nil {
		return cachewire.MigrationFenceResponse{}, err
	}
	out, decodeErr := cachewire.DecodeMigrationFenceResponse(frame.Metadata)
	if decodeErr != nil {
		return out, fmt.Errorf("decode cache migration fence response: %w", decodeErr)
	}
	return out, nil
}
