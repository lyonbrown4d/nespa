package tcp

import (
	"context"
	"fmt"
	"io"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

type PipelineConn struct {
	conn  io.ReadWriteCloser
	codec *protocol.Codec
}

// OpenPipeline opens a persistent TCP connection for pipelined frame round trips.
func (c *Client) OpenPipeline(ctx context.Context, addr string) (*PipelineConn, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &PipelineConn{conn: conn, codec: c.codec}, nil
}

// NewPipelineSetFrame builds a SET frame with a unique request id.
func (c *Client) NewPipelineSetFrame(request cachewire.SetRequest) protocol.Frame {
	return protocol.Frame{
		Op:         protocol.OpCacheSet,
		RequestID:  c.nextID.Add(1),
		RouteEpoch: request.RouteEpoch,
		Metadata:   cachewire.EncodeSetRequest(request),
		Payload:    request.Value,
	}
}

// NewPipelineGetFrame builds a GET frame with a unique request id.
func (c *Client) NewPipelineGetFrame(request cachewire.GetRequest) protocol.Frame {
	return protocol.Frame{
		Op:         protocol.OpCacheGet,
		RequestID:  c.nextID.Add(1),
		RouteEpoch: request.RouteEpoch,
		Metadata:   cachewire.EncodeGetRequest(request),
	}
}

// RoundTrip writes all request frames before reading their responses.
func (p *PipelineConn) RoundTrip(ctx context.Context, frames []protocol.Frame) ([]protocol.Frame, error) {
	if len(frames) == 0 {
		return nil, nil
	}

	positions, err := pipelineFramePositions(frames)
	if err != nil {
		return nil, err
	}
	if err := p.writePipelineFrames(ctx, frames); err != nil {
		return nil, err
	}
	return p.readPipelineResponses(ctx, positions, len(frames))
}

func pipelineFramePositions(frames []protocol.Frame) (map[uint64]int, error) {
	positions := make(map[uint64]int, len(frames))
	for index := range frames {
		requestID := frames[index].RequestID
		if _, ok := positions[requestID]; ok {
			return nil, fmt.Errorf("duplicate cache frame request id: %d", requestID)
		}
		positions[requestID] = index
	}
	return positions, nil
}

func (p *PipelineConn) writePipelineFrames(ctx context.Context, frames []protocol.Frame) error {
	for index := range frames {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("write cache pipeline frame: %w", err)
		}
		if err := p.codec.Encode(p.conn, frames[index]); err != nil {
			return fmt.Errorf("write cache pipeline frame: %w", err)
		}
	}
	return nil
}

func (p *PipelineConn) readPipelineResponses(
	ctx context.Context,
	positions map[uint64]int,
	count int,
) ([]protocol.Frame, error) {
	responses := make([]protocol.Frame, count)
	seen := make(map[uint64]struct{}, count)
	for range count {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read cache pipeline frame: %w", err)
		}
		response, err := p.codec.Decode(p.conn)
		if err != nil {
			return nil, fmt.Errorf("read cache pipeline frame: %w", err)
		}
		if err := recordPipelineResponse(responses, seen, positions, response); err != nil {
			return nil, err
		}
	}
	return responses, nil
}

func recordPipelineResponse(
	responses []protocol.Frame,
	seen map[uint64]struct{},
	positions map[uint64]int,
	response protocol.Frame,
) error {
	index, ok := positions[response.RequestID]
	if !ok {
		return fmt.Errorf("cache pipeline response request id not in batch: %d", response.RequestID)
	}
	if _, ok := seen[response.RequestID]; ok {
		return fmt.Errorf("duplicate cache pipeline response request id: %d", response.RequestID)
	}
	seen[response.RequestID] = struct{}{}
	responses[index] = response
	return nil
}

// Close closes the underlying pipeline connection.
func (p *PipelineConn) Close() error {
	if p == nil || p.conn == nil {
		return nil
	}
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("close cache pipeline connection: %w", err)
	}
	return nil
}
