package cachewire_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestBinaryBatchSetRejectsTruncatedPayload(t *testing.T) {
	request := cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "a"}, Value: []byte("alpha")},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "b"}, Value: []byte("beta")},
	}}
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		t.Fatalf("encode batch set request: %v", err)
	}

	_, err = cachewire.DecodeBatchSetRequest(metadata, payload[:len(payload)-1])
	if !errors.Is(err, cachewire.ErrInvalidPayloadRange) {
		t.Fatalf("truncated payload err = %v, want ErrInvalidPayloadRange", err)
	}
}

func TestBinaryBatchGetResponseRejectsTruncatedPayload(t *testing.T) {
	response := cachewire.BatchGetResponse{Records: []cachewire.Record{
		{Found: true, Namespace: "orders", Space: "session", Key: "a", Value: []byte("alpha")},
		{Found: true, Namespace: "orders", Space: "session", Key: "b", Value: []byte("beta")},
	}}
	metadata, payload, err := cachewire.EncodeBatchGetResponse(response)
	if err != nil {
		t.Fatalf("encode batch get response: %v", err)
	}

	_, err = cachewire.DecodeBatchGetResponse(metadata, payload[:len(payload)-1])
	if !errors.Is(err, cachewire.ErrInvalidPayloadRange) {
		t.Fatalf("truncated payload err = %v, want ErrInvalidPayloadRange", err)
	}
}

func TestBinaryDecodedPayloadsAreDetached(t *testing.T) {
	request := cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "a"}, Value: []byte("alpha")},
	}}
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		t.Fatalf("encode batch set request: %v", err)
	}

	out, err := cachewire.DecodeBatchSetRequest(metadata, payload)
	if err != nil {
		t.Fatalf("decode batch set request: %v", err)
	}
	copy(payload, bytes.Repeat([]byte("x"), len(payload)))

	if got := string(out.Items[0].Value); got != "alpha" {
		t.Fatalf("decoded value = %q, want detached value %q", got, "alpha")
	}
}

func TestBinaryBatchAllowsEmptyPayloadItem(t *testing.T) {
	request := cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "empty"}, Value: nil},
		{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "non-empty"}, Value: []byte("value")},
	}}
	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		t.Fatalf("encode batch set request: %v", err)
	}

	out, err := cachewire.DecodeBatchSetRequest(metadata, payload)
	if err != nil {
		t.Fatalf("decode batch set request: %v", err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("items len = %d, want 2", len(out.Items))
	}
	if len(out.Items[0].Value) != 0 {
		t.Fatalf("empty value len = %d, want 0", len(out.Items[0].Value))
	}
	if string(out.Items[1].Value) != "value" {
		t.Fatalf("second value = %q, want value", string(out.Items[1].Value))
	}
}
