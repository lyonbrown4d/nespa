package cachewire_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestBinarySetRequestRoundTrip(t *testing.T) {
	in := cachewire.SetRequest{
		Key: cachewire.Key{
			Namespace: "orders",
			Space:     "session",
			Entity:    "OrderSession",
			Key:       "order:10086",
		},
		RouteEpoch:       7,
		Value:            []byte("ignored"),
		TTLMillis:        1500,
		NamespaceVersion: 2,
		SpaceVersion:     3,
		ExpectedVersion:  4,
	}

	out, err := cachewire.DecodeSetRequest(cachewire.EncodeSetRequest(in))
	if err != nil {
		t.Fatalf("decode set request: %v", err)
	}
	if out.Key != in.Key || out.TTLMillis != in.TTLMillis || out.NamespaceVersion != in.NamespaceVersion || out.SpaceVersion != in.SpaceVersion || out.ExpectedVersion != in.ExpectedVersion {
		t.Fatalf("set request = %+v, want %+v", out, in)
	}
	if out.RouteEpoch != 0 || len(out.Value) != 0 {
		t.Fatalf("unexpected transport-only fields in metadata: %+v", out)
	}
}

func TestBinaryGetRequestRoundTrip(t *testing.T) {
	key := binaryTestKey()
	get, err := cachewire.DecodeGetRequest(cachewire.EncodeGetRequest(cachewire.GetRequest{
		Key:              key,
		NamespaceVersion: 2,
		SpaceVersion:     3,
	}))
	if err != nil {
		t.Fatalf("decode get request: %v", err)
	}
	if get.Key != key || get.NamespaceVersion != 2 || get.SpaceVersion != 3 {
		t.Fatalf("get request = %+v", get)
	}
}

func TestBinaryDeleteRequestRoundTrip(t *testing.T) {
	key := binaryTestKey()
	del, err := cachewire.DecodeDeleteRequest(cachewire.EncodeDeleteRequest(cachewire.DeleteRequest{Key: key, ExpectedVersion: 9}))
	if err != nil {
		t.Fatalf("decode delete request: %v", err)
	}
	if del.Key != key || del.ExpectedVersion != 9 {
		t.Fatalf("delete request = %+v", del)
	}
}

func TestBinaryExistsRequestRoundTrip(t *testing.T) {
	key := binaryTestKey()
	exists, err := cachewire.DecodeExistsRequest(cachewire.EncodeExistsRequest(cachewire.ExistsRequest{
		Key:              key,
		NamespaceVersion: 4,
		SpaceVersion:     5,
	}))
	if err != nil {
		t.Fatalf("decode exists request: %v", err)
	}
	if exists.Key != key || exists.NamespaceVersion != 4 || exists.SpaceVersion != 5 {
		t.Fatalf("exists request = %+v", exists)
	}
}

func TestBinaryTouchRequestRoundTrip(t *testing.T) {
	key := binaryTestKey()
	touch, err := cachewire.DecodeTouchRequest(cachewire.EncodeTouchRequest(cachewire.TouchRequest{
		Key:              key,
		TTLMillis:        1000,
		NamespaceVersion: 6,
		SpaceVersion:     7,
		ExpectedVersion:  8,
	}))
	if err != nil {
		t.Fatalf("decode touch request: %v", err)
	}
	if touch.Key != key || touch.TTLMillis != 1000 || touch.NamespaceVersion != 6 || touch.SpaceVersion != 7 || touch.ExpectedVersion != 8 {
		t.Fatalf("touch request = %+v", touch)
	}
}

func TestBinaryAdjustRequestRoundTrip(t *testing.T) {
	key := binaryTestKey()
	in := cachewire.AdjustRequest{
		Key:              key,
		TTLMillis:        1500,
		InitialValue:     2,
		Delta:            -3,
		NamespaceVersion: 7,
		SpaceVersion:     8,
		ExpectedVersion:  9,
	}

	adjust, err := cachewire.DecodeAdjustRequest(cachewire.EncodeAdjustRequest(in))
	if err != nil {
		t.Fatalf("decode adjust request: %v", err)
	}
	if adjust.Key != key || adjust.TTLMillis != 1500 || adjust.InitialValue != 2 || adjust.Delta != -3 || adjust.NamespaceVersion != 7 || adjust.SpaceVersion != 8 || adjust.ExpectedVersion != 9 {
		t.Fatalf("adjust request = %+v", adjust)
	}
}

func TestBinaryRecordRoundTrip(t *testing.T) {
	record := cachewire.Record{
		Found:            true,
		Namespace:        "orders",
		Space:            "session",
		Entity:           "OrderSession",
		Key:              "order:10086",
		Version:          9,
		NamespaceVersion: 2,
		SpaceVersion:     3,
		ExpireAtUnixMs:   123456,
		Value:            []byte("ignored"),
	}

	out, err := cachewire.DecodeRecord(cachewire.EncodeRecord(record))
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if out.Value != nil {
		t.Fatalf("record metadata included payload: %+v", out)
	}
	record.Value = nil
	if !reflect.DeepEqual(out, record) {
		t.Fatalf("record = %+v, want %+v", out, record)
	}

	miss, err := cachewire.DecodeRecord(cachewire.EncodeRecord(cachewire.Record{Found: false}))
	if err != nil {
		t.Fatalf("decode miss: %v", err)
	}
	if miss.Found {
		t.Fatalf("miss record = %+v", miss)
	}
}

func TestBinaryBooleanResponsesRoundTrip(t *testing.T) {
	deleted, err := cachewire.DecodeDeleteResponse(cachewire.EncodeDeleteResponse(cachewire.DeleteResponse{Deleted: true}))
	if err != nil || !deleted.Deleted {
		t.Fatalf("delete response = %+v err=%v", deleted, err)
	}
	exists, err := cachewire.DecodeExistsResponse(cachewire.EncodeExistsResponse(cachewire.ExistsResponse{Exists: true}))
	if err != nil || !exists.Exists {
		t.Fatalf("exists response = %+v err=%v", exists, err)
	}
	touched, err := cachewire.DecodeTouchResponse(cachewire.EncodeTouchResponse(cachewire.TouchResponse{Touched: true}))
	if err != nil || !touched.Touched {
		t.Fatalf("touch response = %+v err=%v", touched, err)
	}
}

func TestBinaryMetadataRejectsInvalidInput(t *testing.T) {
	if _, err := cachewire.DecodeGetRequest(nil); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("empty metadata err = %v", err)
	}
	if _, err := cachewire.DecodeGetRequest([]byte{99}); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("version metadata err = %v", err)
	}
	if _, err := cachewire.DecodeDeleteResponse([]byte{1, 2}); !errors.Is(err, cachewire.ErrInvalidMetadata) {
		t.Fatalf("trailing metadata err = %v", err)
	}
}

func binaryTestKey() cachewire.Key {
	return cachewire.Key{Namespace: "orders", Space: "session", Entity: "OrderSession", Key: "order:10086"}
}
