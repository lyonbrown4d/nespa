package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
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
