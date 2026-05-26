package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func TestClientPipelineWritesRequestsBeforeReadingResponses(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "pipeline", Space: "session", Key: "k"}

	pipeline, err := client.OpenPipeline(t.Context(), server.Addr())
	if err != nil {
		t.Fatalf("open pipeline: %v", err)
	}
	defer closePipeline(t, pipeline)

	setFrame := client.NewPipelineSetFrame(cachewire.SetRequest{
		Key:       key,
		Value:     []byte("value"),
		TTLMillis: 30_000,
	})
	getFrame := client.NewPipelineGetFrame(cachewire.GetRequest{Key: key})
	missingFrame := client.NewPipelineGetFrame(cachewire.GetRequest{
		Key: cachewire.Key{Namespace: "pipeline", Space: "session", Key: "missing"},
	})

	responses, err := pipeline.RoundTrip(t.Context(), []protocol.Frame{
		setFrame,
		getFrame,
		missingFrame,
	})
	if err != nil {
		t.Fatalf("pipeline round trip: %v", err)
	}

	requirePipelineRecord(t, responses[0], setFrame.RequestID, "value", true)
	requirePipelineRecord(t, responses[1], getFrame.RequestID, "value", true)
	requirePipelineRecord(t, responses[2], missingFrame.RequestID, "", false)
}

func requirePipelineRecord(
	t *testing.T,
	frame protocol.Frame,
	requestID uint64,
	wantValue string,
	wantFound bool,
) {
	t.Helper()

	if frame.RequestID != requestID {
		t.Fatalf("response request id = %d, want %d", frame.RequestID, requestID)
	}
	if frame.Flags&protocol.FlagError != 0 {
		t.Fatalf("response %d is error: %s", requestID, frame.Metadata)
	}
	record, err := cachewire.DecodeRecord(frame.Metadata)
	if err != nil {
		t.Fatalf("decode pipeline record: %v", err)
	}
	if len(frame.Payload) > 0 {
		record.Value = append(record.Value[:0], frame.Payload...)
	}
	if record.Found != wantFound {
		t.Fatalf("record found = %t, want %t", record.Found, wantFound)
	}
	if string(record.Value) != wantValue {
		t.Fatalf("record value = %q, want %q", record.Value, wantValue)
	}
}

func closePipeline(t *testing.T, pipeline interface{ Close() error }) {
	t.Helper()

	if err := pipeline.Close(); err != nil {
		t.Fatalf("close pipeline: %v", err)
	}
}
