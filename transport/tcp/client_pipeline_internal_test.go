package tcp

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/protocol"
)

func TestPipelineRoundTripWritesAllFramesBeforeReadingAndOrdersResponses(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer closeConn(clientConn)
	defer closeConn(serverConn)

	codec := protocol.NewCodec()
	pipeline := &PipelineConn{conn: clientConn, codec: codec}
	requests := pipelineInternalRequests()

	serverErr := make(chan error, 1)
	go serveOutOfOrderPipeline(t, serverConn, codec, len(requests), serverErr)

	result := waitPipelineRoundTrip(t, pipeline, requests)
	requirePipelineInternalOrder(t, result, requests)
	if err := <-serverErr; err != nil {
		t.Fatalf("fake server: %v", err)
	}
}

func pipelineInternalRequests() []protocol.Frame {
	return []protocol.Frame{
		{Op: protocol.OpCacheGet, RequestID: 10, Metadata: []byte("first")},
		{Op: protocol.OpCacheSet, RequestID: 11, Metadata: []byte("second")},
		{Op: protocol.OpCacheGet, RequestID: 12, Metadata: []byte("third")},
	}
}

func serveOutOfOrderPipeline(
	t *testing.T,
	conn net.Conn,
	codec *protocol.Codec,
	count int,
	errc chan<- error,
) {
	t.Helper()

	received, err := readPipelineInternalRequests(conn, codec, count)
	if err != nil {
		errc <- err
		return
	}
	errc <- writePipelineInternalResponses(conn, codec, received)
}

func readPipelineInternalRequests(
	conn net.Conn,
	codec *protocol.Codec,
	count int,
) ([]protocol.Frame, error) {
	received := make([]protocol.Frame, count)
	for index := range received {
		frame, err := codec.Decode(conn)
		if err != nil {
			return nil, fmt.Errorf("decode pipeline request: %w", err)
		}
		received[index] = frame
	}
	return received, nil
}

func writePipelineInternalResponses(
	conn net.Conn,
	codec *protocol.Codec,
	received []protocol.Frame,
) error {
	for index, frame := range slices.Backward(received) {
		response := protocol.Frame{
			Flags:     protocol.FlagResponse,
			Op:        frame.Op,
			RequestID: frame.RequestID,
			Metadata:  []byte{strconv.Itoa(index)[0]},
		}
		if err := codec.Encode(conn, response); err != nil {
			return fmt.Errorf("encode pipeline response: %w", err)
		}
	}
	return nil
}

type pipelineRoundTripResult struct {
	responses []protocol.Frame
	err       error
}

func waitPipelineRoundTrip(
	t *testing.T,
	pipeline *PipelineConn,
	requests []protocol.Frame,
) pipelineRoundTripResult {
	t.Helper()

	done := make(chan pipelineRoundTripResult, 1)
	go func() {
		responses, err := pipeline.RoundTrip(context.Background(), requests)
		done <- pipelineRoundTripResult{responses: responses, err: err}
	}()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("pipeline round trip: %v", result.err)
		}
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline round trip blocked")
		return pipelineRoundTripResult{}
	}
}

func requirePipelineInternalOrder(
	t *testing.T,
	result pipelineRoundTripResult,
	requests []protocol.Frame,
) {
	t.Helper()

	for index := range requests {
		if result.responses[index].RequestID != requests[index].RequestID {
			t.Fatalf("response %d request id = %d, want %d",
				index,
				result.responses[index].RequestID,
				requests[index].RequestID,
			)
		}
	}
}
