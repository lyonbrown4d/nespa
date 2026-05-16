package cachewire_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestBinaryPrimitiveRequestRoundTrip(t *testing.T) {
	request := cachewire.PrimitiveRequest{
		Key:              cachewire.Key{Namespace: "orders", Space: "session", Key: "p"},
		RouteEpoch:       7,
		Kind:             cachewire.PrimitiveScoredSetRange,
		TTLMillis:        100,
		NamespaceVersion: 2,
		SpaceVersion:     3,
		ExpectedVersion:  4,
		Field:            "name",
		Member:           "alice",
		Value:            []byte("value"),
		Delta:            -2,
		InitialValue:     9,
		Score:            1.5,
		MinScore:         1,
		MaxScore:         2,
		HasMinScore:      true,
		HasMaxScore:      true,
		Limit:            3,
		Start:            1,
		Reverse:          true,
	}

	metadata, payload, err := cachewire.EncodePrimitiveRequest(request)
	if err != nil {
		t.Fatalf("encode primitive request: %v", err)
	}
	out, err := cachewire.DecodePrimitiveRequest(metadata, payload)
	if err != nil {
		t.Fatalf("decode primitive request: %v", err)
	}
	assertPrimitiveRequest(t, out, request)
}

func TestBinaryPrimitiveResponseRoundTrip(t *testing.T) {
	result := cachewire.PrimitiveResult{
		Record:  cachewire.Record{Found: true, Namespace: "orders", Space: "session", Key: "p", Version: 2},
		Found:   true,
		Applied: true,
		Value:   []byte("alice"),
		Bool:    true,
		Count:   2,
		Fields: []cachewire.MapField{
			{Field: "name", Value: []byte("alice")},
			{Field: "role", Value: []byte("admin")},
		},
		Members: []string{"blue", "red"},
		ScoredMembers: []cachewire.ScoredMember{
			{Member: "alice", Score: 2},
			{Member: "bob", Score: 3},
		},
		Values: []cachewire.ListValue{
			{Value: []byte("first")},
			{Value: []byte("second")},
		},
	}

	metadata, payload, err := cachewire.EncodePrimitiveResponse(result)
	if err != nil {
		t.Fatalf("encode primitive response: %v", err)
	}
	out, err := cachewire.DecodePrimitiveResponse(metadata, payload)
	if err != nil {
		t.Fatalf("decode primitive response: %v", err)
	}
	assertPrimitiveResult(t, out, result)
}

func TestBinaryBatchPrimitiveRoundTrip(t *testing.T) {
	request := cachewire.BatchPrimitiveRequest{Items: []cachewire.PrimitiveRequest{
		{Kind: cachewire.PrimitiveMapSet, Key: binaryPrimitiveKey(), Field: "name", Value: []byte("alice")},
		{Kind: cachewire.PrimitiveSetAdd, Key: binaryPrimitiveKey(), Member: "blue"},
	}}
	response := cachewire.BatchPrimitiveResponse{Results: []cachewire.PrimitiveResult{
		{Found: true, Applied: true, Count: 1},
		{Found: true, Applied: true, Bool: true, Count: 1},
	}}

	encodedRequest, requestPayload, err := cachewire.EncodeBatchPrimitiveRequest(request)
	if err != nil {
		t.Fatalf("encode batch primitive request: %v", err)
	}
	decodedRequest, err := cachewire.DecodeBatchPrimitiveRequest(encodedRequest, requestPayload)
	if err != nil {
		t.Fatalf("decode batch primitive request: %v", err)
	}
	assertPrimitiveRequest(t, decodedRequest.Items[0], request.Items[0])

	encodedResponse, responsePayload, err := cachewire.EncodeBatchPrimitiveResponse(response)
	if err != nil {
		t.Fatalf("encode batch primitive response: %v", err)
	}
	decodedResponse, err := cachewire.DecodeBatchPrimitiveResponse(encodedResponse, responsePayload)
	if err != nil {
		t.Fatalf("decode batch primitive response: %v", err)
	}
	if len(decodedResponse.Results) != len(response.Results) {
		t.Fatalf("result len = %d, want %d", len(decodedResponse.Results), len(response.Results))
	}
}

func assertPrimitiveRequest(t *testing.T, got, want cachewire.PrimitiveRequest) {
	t.Helper()
	if got.RouteEpoch != 0 || !bytes.Equal(got.Value, want.Value) {
		t.Fatalf("primitive request transport fields = %+v, want value %q", got, want.Value)
	}
	got.RouteEpoch = 0
	got.Value = nil
	got.PayloadOffset = 0
	got.PayloadSize = 0
	want.RouteEpoch = 0
	want.Value = nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("primitive request = %+v, want %+v", got, want)
	}
}

func assertPrimitiveResult(t *testing.T, got, want cachewire.PrimitiveResult) {
	t.Helper()
	if got.Found != want.Found || got.Applied != want.Applied || got.Bool != want.Bool || got.Count != want.Count {
		t.Fatalf("primitive result metadata = %+v, want %+v", got, want)
	}
	if !bytes.Equal(got.Value, want.Value) || len(got.Fields) != len(want.Fields) {
		t.Fatalf("primitive result payload = %+v, want %+v", got, want)
	}
	assertPrimitiveFields(t, got.Fields, want.Fields)
	assertPrimitiveValues(t, got.Values, want.Values)
}

func assertPrimitiveFields(t *testing.T, got, want []cachewire.MapField) {
	t.Helper()
	for index := range want {
		if got[index].Field != want[index].Field || !bytes.Equal(got[index].Value, want[index].Value) {
			t.Fatalf("field[%d] = %+v, want %+v", index, got[index], want[index])
		}
	}
}

func assertPrimitiveValues(t *testing.T, got, want []cachewire.ListValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values len = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if !bytes.Equal(got[index].Value, want[index].Value) {
			t.Fatalf("value[%d] = %q, want %q", index, got[index].Value, want[index].Value)
		}
	}
}

func binaryPrimitiveKey() cachewire.Key {
	return cachewire.Key{Namespace: "orders", Space: "session", Key: "p"}
}
