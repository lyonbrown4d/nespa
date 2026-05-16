package nespa

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestClientSetMapsUserRequest(t *testing.T) {
	backend := &fakeClient{
		setRecord: cachewire.Record{
			Found:            true,
			Namespace:        "orders",
			Space:            "session",
			Entity:           "SessionView",
			Key:              "k1",
			Value:            []byte("stored"),
			Version:          2,
			NamespaceVersion: 3,
			SpaceVersion:     4,
			ExpireAtUnixMs:   1000,
		},
	}
	sdk := &Client{backend: backend}

	record, err := sdk.Set(context.Background(), Key{
		Namespace: "orders",
		Space:     "session",
		Entity:    "SessionView",
		Key:       "k1",
	}, []byte("value"), SetOptions{
		TTL:              1500 * time.Millisecond,
		NamespaceVersion: 3,
		SpaceVersion:     4,
		ExpectedVersion:  1,
	})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if backend.setRequest.TTLMillis != 1500 {
		t.Fatalf("ttl millis = %d, want 1500", backend.setRequest.TTLMillis)
	}
	if backend.setRequest.ExpectedVersion != 1 {
		t.Fatalf("expected version = %d, want 1", backend.setRequest.ExpectedVersion)
	}
	if string(backend.setRequest.Value) != "value" {
		t.Fatalf("value = %q, want value", backend.setRequest.Value)
	}
	if !record.Found || record.Key.Namespace != "orders" || string(record.Value) != "stored" {
		t.Fatalf("record = %+v", record)
	}
	if record.ExpireAt != time.UnixMilli(1000) {
		t.Fatalf("expire at = %v, want %v", record.ExpireAt, time.UnixMilli(1000))
	}
}

func TestClientAdjustMapsUserRequest(t *testing.T) {
	backend := &fakeClient{
		adjustRecord: cachewire.Record{Found: true, Key: "counter", Value: []byte("12"), Version: 1},
	}
	sdk := &Client{backend: backend}

	record, err := sdk.Adjust(context.Background(), Key{Namespace: "n", Space: "s", Key: "counter"}, AdjustOptions{
		InitialValue:    10,
		Delta:           2,
		ExpectedVersion: 7,
	})
	if err != nil {
		t.Fatalf("adjust: %v", err)
	}
	if backend.adjustRequest.InitialValue != 10 || backend.adjustRequest.Delta != 2 {
		t.Fatalf("adjust request = %+v", backend.adjustRequest)
	}
	if backend.adjustRequest.ExpectedVersion != 7 {
		t.Fatalf("expected version = %d, want 7", backend.adjustRequest.ExpectedVersion)
	}
	if string(record.Value) != "12" {
		t.Fatalf("record value = %q, want 12", record.Value)
	}
}

func TestErrorCodeOf(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", cachewire.Error{Code: ErrorInvalidArgument, Message: "bad counter"})

	code, ok := ErrorCodeOf(err)
	if !ok {
		t.Fatal("expected wire error code")
	}
	if code != ErrorInvalidArgument {
		t.Fatalf("code = %d, want %d", code, ErrorInvalidArgument)
	}

	if _, ok := ErrorCodeOf(errors.New("plain")); ok {
		t.Fatal("plain error should not expose a wire error code")
	}
}

type fakeClient struct {
	setRequest    cachewire.SetRequest
	setRecord     cachewire.Record
	adjustRequest cachewire.AdjustRequest
	adjustRecord  cachewire.Record
}

func (f *fakeClient) Set(_ context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
	f.setRequest = request
	return f.setRecord, nil
}

func (f *fakeClient) Get(context.Context, cachewire.GetRequest) (cachewire.Record, error) {
	return cachewire.Record{}, nil
}

func (f *fakeClient) Delete(context.Context, cachewire.DeleteRequest) (cachewire.DeleteResponse, error) {
	return cachewire.DeleteResponse{}, nil
}

func (f *fakeClient) Exists(context.Context, cachewire.ExistsRequest) (cachewire.ExistsResponse, error) {
	return cachewire.ExistsResponse{}, nil
}

func (f *fakeClient) Touch(context.Context, cachewire.TouchRequest) (cachewire.TouchResponse, error) {
	return cachewire.TouchResponse{}, nil
}

func (f *fakeClient) Adjust(_ context.Context, request cachewire.AdjustRequest) (cachewire.Record, error) {
	f.adjustRequest = request
	return f.adjustRecord, nil
}

func (f *fakeClient) BatchSet(context.Context, cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error) {
	return cachewire.BatchSetResponse{}, nil
}

func (f *fakeClient) BatchGet(context.Context, cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error) {
	return cachewire.BatchGetResponse{}, nil
}
