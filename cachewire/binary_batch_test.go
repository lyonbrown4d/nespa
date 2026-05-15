package cachewire_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestBinaryBatchSetRequestRoundTrip(t *testing.T) {
	request := cachewire.BatchSetRequest{
		RouteEpoch: 7,
		Items: []cachewire.SetRequest{
			{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "a"}, Value: []byte("alpha"), TTLMillis: 100, NamespaceVersion: 2, SpaceVersion: 3, ExpectedVersion: 9},
			{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "b"}, Value: []byte("beta"), TTLMillis: 200, NamespaceVersion: 2, SpaceVersion: 3, ExpectedVersion: 10},
		},
	}

	metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
	if err != nil {
		t.Fatalf("encode batch set: %v", err)
	}
	out, err := cachewire.DecodeBatchSetRequest(metadata, payload)
	if err != nil {
		t.Fatalf("decode batch set: %v", err)
	}
	if out.RouteEpoch != 0 {
		t.Fatalf("route epoch leaked into metadata: %+v", out)
	}
	requireSetItems(t, out.Items, request.Items)
}

func TestBinaryBatchGetRequestRoundTrip(t *testing.T) {
	request := cachewire.BatchGetRequest{
		RouteEpoch: 9,
		Items: []cachewire.GetRequest{
			{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "a"}, NamespaceVersion: 2, SpaceVersion: 3},
			{Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "b"}, NamespaceVersion: 4, SpaceVersion: 5},
		},
	}

	out, err := cachewire.DecodeBatchGetRequest(cachewire.EncodeBatchGetRequest(request))
	if err != nil {
		t.Fatalf("decode batch get: %v", err)
	}
	if out.RouteEpoch != 0 {
		t.Fatalf("route epoch leaked into metadata: %+v", out)
	}
	if !reflect.DeepEqual(out.Items, request.Items) {
		t.Fatalf("items = %+v, want %+v", out.Items, request.Items)
	}
}

func TestBinaryBatchSetResponseRoundTrip(t *testing.T) {
	response := cachewire.BatchSetResponse{Records: []cachewire.Record{
		{Found: true, Namespace: "orders", Space: "session", Key: "a", Version: 1, NamespaceVersion: 2, SpaceVersion: 3},
		{Found: true, Namespace: "orders", Space: "session", Key: "b", Version: 1, NamespaceVersion: 2, SpaceVersion: 3},
	}}

	out, err := cachewire.DecodeBatchSetResponse(cachewire.EncodeBatchSetResponse(response))
	if err != nil {
		t.Fatalf("decode batch set response: %v", err)
	}
	if !reflect.DeepEqual(out, response) {
		t.Fatalf("response = %+v, want %+v", out, response)
	}
}

func TestBinaryBatchGetResponseRoundTrip(t *testing.T) {
	response := cachewire.BatchGetResponse{Records: []cachewire.Record{
		{Found: true, Namespace: "orders", Space: "session", Key: "a", Version: 1, NamespaceVersion: 2, SpaceVersion: 3, Value: []byte("alpha")},
		{Found: false},
		{Found: true, Namespace: "orders", Space: "session", Key: "b", Version: 1, NamespaceVersion: 2, SpaceVersion: 3, Value: []byte("beta")},
	}}

	metadata, payload, err := cachewire.EncodeBatchGetResponse(response)
	if err != nil {
		t.Fatalf("encode batch get response: %v", err)
	}
	out, err := cachewire.DecodeBatchGetResponse(metadata, payload)
	if err != nil {
		t.Fatalf("decode batch get response: %v", err)
	}
	clearPayloadRanges(out.Records)
	clearPayloadRanges(response.Records)
	if !reflect.DeepEqual(out, response) {
		t.Fatalf("response = %+v, want %+v", out, response)
	}
}

func TestBinaryBatchRejectsInvalidMetadata(t *testing.T) {
	if _, err := cachewire.DecodeBatchGetRequest(nil); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("empty metadata err = %v", err)
	}
	if _, err := cachewire.DecodeBatchSetRequest([]byte{1, 1}, nil); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("truncated metadata err = %v", err)
	}
	if _, err := cachewire.DecodeBatchGetResponse([]byte{1, 1, 1}, nil); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("invalid response metadata err = %v", err)
	}
}

func requireSetItems(t *testing.T, got, want []cachewire.SetRequest) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("items len = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Key != want[index].Key || !bytes.Equal(got[index].Value, want[index].Value) {
			t.Fatalf("item[%d] = %+v, want %+v", index, got[index], want[index])
		}
		if got[index].TTLMillis != want[index].TTLMillis || got[index].NamespaceVersion != want[index].NamespaceVersion || got[index].SpaceVersion != want[index].SpaceVersion || got[index].ExpectedVersion != want[index].ExpectedVersion {
			t.Fatalf("item[%d] metadata = %+v, want %+v", index, got[index], want[index])
		}
	}
}

func clearPayloadRanges(records []cachewire.Record) {
	for index := range records {
		records[index].PayloadOffset = 0
		records[index].PayloadSize = 0
	}
}
