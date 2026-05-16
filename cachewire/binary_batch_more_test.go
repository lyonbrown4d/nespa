package cachewire_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestBinaryBatchDeleteRequestRoundTrip(t *testing.T) {
	request := cachewire.BatchDeleteRequest{Items: []cachewire.DeleteRequest{
		{Key: wireKey("a"), ExpectedVersion: 2},
		{Key: wireKey("b"), ExpectedVersion: 3},
	}}
	out, err := cachewire.DecodeBatchDeleteRequest(cachewire.EncodeBatchDeleteRequest(request))
	if err != nil {
		t.Fatalf("decode batch delete: %v", err)
	}
	if len(out.Items) != 2 || out.Items[0].Key != request.Items[0].Key || out.Items[1].ExpectedVersion != 3 {
		t.Fatalf("batch delete roundtrip = %+v", out)
	}
}

func TestBinaryBatchExistsRequestRoundTrip(t *testing.T) {
	request := cachewire.BatchExistsRequest{Items: []cachewire.ExistsRequest{
		{Key: wireKey("a"), NamespaceVersion: 4, SpaceVersion: 5},
	}}
	out, err := cachewire.DecodeBatchExistsRequest(cachewire.EncodeBatchExistsRequest(request))
	if err != nil {
		t.Fatalf("decode batch exists: %v", err)
	}
	if len(out.Items) != 1 || out.Items[0].NamespaceVersion != 4 || out.Items[0].SpaceVersion != 5 {
		t.Fatalf("batch exists roundtrip = %+v", out)
	}
}

func TestBinaryBatchTouchRequestRoundTrip(t *testing.T) {
	request := cachewire.BatchTouchRequest{Items: []cachewire.TouchRequest{
		{Key: wireKey("a"), TTLMillis: 100, NamespaceVersion: 4, SpaceVersion: 5, ExpectedVersion: 6},
	}}
	out, err := cachewire.DecodeBatchTouchRequest(cachewire.EncodeBatchTouchRequest(request))
	if err != nil {
		t.Fatalf("decode batch touch: %v", err)
	}
	if len(out.Items) != 1 || out.Items[0].TTLMillis != 100 || out.Items[0].ExpectedVersion != 6 {
		t.Fatalf("batch touch roundtrip = %+v", out)
	}
}

func TestBinaryBatchBoolResponsesRoundTrip(t *testing.T) {
	assertBatchDeleteResponse(t)
	assertBatchExistsResponse(t)
	assertBatchTouchResponse(t)
}

func assertBatchDeleteResponse(t *testing.T) {
	t.Helper()
	response := cachewire.BatchDeleteResponse{Results: []cachewire.DeleteResponse{
		{Deleted: true},
		{Deleted: false},
	}}
	out, err := cachewire.DecodeBatchDeleteResponse(cachewire.EncodeBatchDeleteResponse(response))
	if err != nil {
		t.Fatalf("decode batch delete response: %v", err)
	}
	if len(out.Results) != 2 || !out.Results[0].Deleted || out.Results[1].Deleted {
		t.Fatalf("batch delete response = %+v", out)
	}
}

func assertBatchExistsResponse(t *testing.T) {
	t.Helper()
	response := cachewire.BatchExistsResponse{Results: []cachewire.ExistsResponse{{Exists: true}}}
	out, err := cachewire.DecodeBatchExistsResponse(cachewire.EncodeBatchExistsResponse(response))
	if err != nil {
		t.Fatalf("decode batch exists response: %v", err)
	}
	if len(out.Results) != 1 || !out.Results[0].Exists {
		t.Fatalf("batch exists response = %+v", out)
	}
}

func assertBatchTouchResponse(t *testing.T) {
	t.Helper()
	response := cachewire.BatchTouchResponse{Results: []cachewire.TouchResponse{{Touched: true}}}
	out, err := cachewire.DecodeBatchTouchResponse(cachewire.EncodeBatchTouchResponse(response))
	if err != nil {
		t.Fatalf("decode batch touch response: %v", err)
	}
	if len(out.Results) != 1 || !out.Results[0].Touched {
		t.Fatalf("batch touch response = %+v", out)
	}
}

func wireKey(key string) cachewire.Key {
	return cachewire.Key{Namespace: "orders", Space: "session", Key: key}
}
