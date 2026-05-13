package protocol_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/lyonbrown4d/nespa/internal/protocol"
)

func TestCodecRoundTrip(t *testing.T) {
	codec := protocol.NewCodec()
	var buf bytes.Buffer

	in := protocol.Frame{
		Flags:      protocol.FlagResponse | protocol.FlagMore,
		Op:         protocol.OpCacheSet,
		RequestID:  42,
		RouteEpoch: 7,
		Metadata:   []byte(`{"namespace":"orders","space":"session"}`),
		Payload:    []byte("payload"),
	}
	if err := codec.Encode(&buf, in); err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := codec.Decode(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Flags != in.Flags || out.Op != in.Op || out.RequestID != in.RequestID || out.RouteEpoch != in.RouteEpoch {
		t.Fatalf("unexpected frame header: %+v", out)
	}
	if !bytes.Equal(out.Metadata, in.Metadata) {
		t.Fatalf("metadata = %q, want %q", out.Metadata, in.Metadata)
	}
	if !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("payload = %q, want %q", out.Payload, in.Payload)
	}
}

func TestCodecHeaderLayout(t *testing.T) {
	var buf bytes.Buffer
	metaLen := uint32(4)
	payloadLen := uint32(4)
	frame := protocol.Frame{
		Flags:      protocol.FlagError,
		Op:         protocol.OpCacheGet,
		RequestID:  100,
		RouteEpoch: 200,
		Metadata:   []byte("meta"),
		Payload:    []byte("body"),
	}
	if err := protocol.Encode(&buf, frame); err != nil {
		t.Fatalf("encode: %v", err)
	}

	raw := buf.Bytes()
	requireEqual(t, "len", len(raw), protocol.FixedHeaderSize+len(frame.Metadata)+len(frame.Payload))
	requireEqual(t, "magic", binary.BigEndian.Uint32(raw[0:4]), protocol.Magic)
	requireEqual(t, "version", raw[4], protocol.Version)
	requireEqual(t, "flags", protocol.Flags(raw[5]), protocol.FlagError)
	requireEqual(t, "op", protocol.Op(binary.BigEndian.Uint16(raw[6:8])), protocol.OpCacheGet)
	requireEqual(t, "request id", binary.BigEndian.Uint64(raw[8:16]), uint64(100))
	requireEqual(t, "route epoch", binary.BigEndian.Uint64(raw[16:24]), uint64(200))
	requireEqual(t, "metadata len", binary.BigEndian.Uint32(raw[24:28]), metaLen)
	requireEqual(t, "payload len", binary.BigEndian.Uint32(raw[28:32]), payloadLen)
}

func TestCodecRejectsInvalidMagic(t *testing.T) {
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, protocol.Frame{Op: protocol.OpCacheGet}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw := buf.Bytes()
	raw[0] = 0

	_, err := protocol.Decode(bytes.NewReader(raw))
	if !errors.Is(err, protocol.ErrInvalidMagic) {
		t.Fatalf("err = %v, want invalid magic", err)
	}
}

func TestCodecRejectsUnsupportedVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, protocol.Frame{Op: protocol.OpCacheGet}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw := buf.Bytes()
	raw[4] = protocol.Version + 1

	_, err := protocol.Decode(bytes.NewReader(raw))
	if !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("err = %v, want unsupported version", err)
	}
}

func TestCodecRejectsOversizedMetadataBeforeAllocation(t *testing.T) {
	var header [protocol.FixedHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], protocol.Magic)
	header[4] = protocol.Version
	binary.BigEndian.PutUint16(header[6:8], uint16(protocol.OpCacheGet))
	binary.BigEndian.PutUint32(header[24:28], 2)

	codec := protocol.NewCodec(protocol.WithMaxMetadataBytes(1))
	_, err := codec.Decode(bytes.NewReader(header[:]))
	if !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("err = %v, want frame too large", err)
	}
}

func TestCodecRejectsOversizedPayloadOnEncode(t *testing.T) {
	codec := protocol.NewCodec(protocol.WithMaxPayloadBytes(1))
	err := codec.Encode(io.Discard, protocol.Frame{
		Op:      protocol.OpCacheSet,
		Payload: []byte("too large"),
	})
	if !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("err = %v, want frame too large", err)
	}
}

func TestCodecRejectsMissingOp(t *testing.T) {
	var buf bytes.Buffer
	err := protocol.Encode(&buf, protocol.Frame{})
	if !errors.Is(err, protocol.ErrInvalidFrame) {
		t.Fatalf("err = %v, want invalid frame", err)
	}
}

func TestCodecReportsShortRead(t *testing.T) {
	_, err := protocol.Decode(bytes.NewReader([]byte{1, 2, 3}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want unexpected EOF", err)
	}
}

func TestCodecReportsShortWrite(t *testing.T) {
	err := protocol.Encode(shortWriter{}, protocol.Frame{Op: protocol.OpCacheGet})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err = %v, want short write", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func requireEqual[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}
