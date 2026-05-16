package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerBatchSetGet(t *testing.T) {
	server, client := startCacheClientServer(t)
	set, err := client.BatchSet(t.Context(), server.Addr(), cachewire.BatchSetRequest{
		Items: []cachewire.SetRequest{
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "a"}, Value: []byte("alpha")},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "b"}, Value: []byte("beta")},
		},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(set.Records) != 2 || set.Records[0].Version != 1 || set.Records[1].Version != 1 {
		t.Fatalf("unexpected batch set response: %+v", set)
	}

	get, err := client.BatchGet(t.Context(), server.Addr(), cachewire.BatchGetRequest{
		Items: []cachewire.GetRequest{
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "a"}},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "missing"}},
			{Key: cachewire.Key{Namespace: "ns", Space: "sp", Key: "b"}},
		},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	requireBatchGet(t, get)
}

func TestClientServerBatchDeleteExistsTouch(t *testing.T) {
	server, client := startCacheClientServer(t)
	keys := []cachewire.Key{
		{Namespace: "ns", Space: "sp", Key: "a"},
		{Namespace: "ns", Space: "sp", Key: "b"},
	}
	seedBatchRecords(t, client, server.Addr(), keys)

	exists, err := client.BatchExists(t.Context(), server.Addr(), cachewire.BatchExistsRequest{
		Items: []cachewire.ExistsRequest{{Key: keys[0]}, {Key: keys[1]}},
	})
	if err != nil {
		t.Fatalf("batch exists: %v", err)
	}
	requireBatchExists(t, exists)

	touch, err := client.BatchTouch(t.Context(), server.Addr(), cachewire.BatchTouchRequest{
		Items: []cachewire.TouchRequest{{Key: keys[0], TTLMillis: 1000}},
	})
	if err != nil {
		t.Fatalf("batch touch: %v", err)
	}
	requireBatchTouch(t, touch)

	deleted, err := client.BatchDelete(t.Context(), server.Addr(), cachewire.BatchDeleteRequest{
		Items: []cachewire.DeleteRequest{{Key: keys[0]}, {Key: keys[1]}},
	})
	if err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	requireBatchDelete(t, deleted)
}

func requireBatchGet(t *testing.T, get cachewire.BatchGetResponse) {
	t.Helper()
	if len(get.Records) != 3 {
		t.Fatalf("unexpected batch get count: %+v", get)
	}
	if !get.Records[0].Found || string(get.Records[0].Value) != "alpha" {
		t.Fatalf("unexpected first batch get record: %+v", get.Records[0])
	}
	if get.Records[1].Found {
		t.Fatalf("unexpected missing batch get record: %+v", get.Records[1])
	}
	if !get.Records[2].Found || string(get.Records[2].Value) != "beta" {
		t.Fatalf("unexpected second batch get record: %+v", get.Records[2])
	}
}

func requireBatchExists(t *testing.T, exists cachewire.BatchExistsResponse) {
	t.Helper()
	if len(exists.Results) != 2 || !exists.Results[0].Exists || !exists.Results[1].Exists {
		t.Fatalf("unexpected batch exists response: %+v", exists)
	}
}

func requireBatchTouch(t *testing.T, touch cachewire.BatchTouchResponse) {
	t.Helper()
	if len(touch.Results) != 1 || !touch.Results[0].Touched {
		t.Fatalf("unexpected batch touch response: %+v", touch)
	}
}

func requireBatchDelete(t *testing.T, deleted cachewire.BatchDeleteResponse) {
	t.Helper()
	if len(deleted.Results) != 2 || !deleted.Results[0].Deleted || !deleted.Results[1].Deleted {
		t.Fatalf("unexpected batch delete response: %+v", deleted)
	}
}

func seedBatchRecords(t *testing.T, client *cachetcp.Client, addr string, keys []cachewire.Key) {
	t.Helper()
	_, err := client.BatchSet(t.Context(), addr, cachewire.BatchSetRequest{
		Items: []cachewire.SetRequest{
			{Key: keys[0], Value: []byte("alpha")},
			{Key: keys[1], Value: []byte("beta")},
		},
	})
	if err != nil {
		t.Fatalf("seed batch records: %v", err)
	}
}
