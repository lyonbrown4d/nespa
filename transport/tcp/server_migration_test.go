package tcp_test

import (
	"fmt"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	"github.com/lyonbrown4d/nespa/routing"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestClientServerMigratesRangeBetweenNodes(t *testing.T) {
	source, client := startCacheClientServer(t)
	target, _ := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "moving"}

	if _, err := client.Set(t.Context(), source.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	request := migrationRangeForKey(key)
	snapshot, err := client.ExportRange(t.Context(), source.Addr(), request)
	if err != nil {
		t.Fatalf("export source range: %v", err)
	}
	if len(snapshot.Entries) != 1 {
		t.Fatalf("snapshot entries = %d, want 1", len(snapshot.Entries))
	}

	imported, err := client.ImportSnapshot(t.Context(), target.Addr(), snapshot)
	if err != nil {
		t.Fatalf("import target snapshot: %v", err)
	}
	if imported.Imported != 1 {
		t.Fatalf("imported = %d, want 1", imported.Imported)
	}
	requireWireValue(t, client, target.Addr(), key, "payload")

	deleted, err := client.DeleteRange(t.Context(), source.Addr(), request)
	if err != nil {
		t.Fatalf("delete source range: %v", err)
	}
	if deleted.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted.Deleted)
	}
	requireWireMissing(t, client, source.Addr(), key)
}

func TestClientServerFenceRangeBlocksMutationAndAllowsReads(t *testing.T) {
	server, client := startCacheClientServer(t)
	key := cachewire.Key{Namespace: "orders", Space: "session", Entity: "SessionView", Key: "migrate-blocked"}
	rangeRequest := migrationRangeForKey(key)

	if _, err := client.Set(t.Context(), server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("payload"),
	}); err != nil {
		t.Fatalf("seed migration key: %v", err)
	}
	if _, err := client.Get(t.Context(), server.Addr(), cachewire.GetRequest{Key: key}); err != nil {
		t.Fatalf("get before fence: %v", err)
	}

	if _, err := client.FenceRange(t.Context(), server.Addr(), rangeRequest); err != nil {
		t.Fatalf("fence range: %v", err)
	}

	if _, err := client.Get(t.Context(), server.Addr(), cachewire.GetRequest{Key: key}); err != nil {
		t.Fatalf("get inside fence: %v", err)
	}

	_, err := client.Set(t.Context(), server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("updated"),
	})
	requireWireErrorCode(t, err, protocol.ErrorNoRoute)

	outKey := keyOutsideRangeForSlot(t, "orders", "session", rangeRequest.VSlotStart, rangeRequest.VSlotEnd)
	if _, err := client.Set(t.Context(), server.Addr(), cachewire.SetRequest{
		Key:   outKey,
		Value: []byte("untouched"),
	}); err != nil {
		t.Fatalf("set out-of-range key: %v", err)
	}

	if _, err := client.UnfenceRange(t.Context(), server.Addr(), rangeRequest); err != nil {
		t.Fatalf("unfence range: %v", err)
	}
	_, err = client.Set(t.Context(), server.Addr(), cachewire.SetRequest{
		Key:   key,
		Value: []byte("updated"),
	})
	if err != nil {
		t.Fatalf("set after unfence: %v", err)
	}
}

func migrationRangeForKey(key cachewire.Key) cachewire.MigrationRangeRequest {
	slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
	return cachewire.MigrationRangeRequest{
		Namespace:  key.Namespace,
		Space:      key.Space,
		VSlotStart: slot,
		VSlotEnd:   slot,
	}
}

func keyOutsideRangeForSlot(
	t *testing.T,
	namespace, space string,
	start, end uint32,
) cachewire.Key {
	t.Helper()
	for index := range 100_000 {
		key := cachewire.Key{
			Namespace: namespace,
			Space:     space,
			Entity:    "SessionView",
			Key:       fmt.Sprintf("outside-%d", index),
		}
		slot := routing.VSlotFor(namespace, space, key.Key)
		if slot < start || slot > end {
			return key
		}
	}
	t.Fatal("failed to find key outside migration slot range")
	return cachewire.Key{}
}

func requireWireValue(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key, want string) {
	t.Helper()
	record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get wire value: %v", err)
	}
	if !record.Found || string(record.Value) != want {
		t.Fatalf("record = found %v value %q, want %q", record.Found, record.Value, want)
	}
}

func requireWireMissing(t *testing.T, client *cachetcp.Client, addr string, key cachewire.Key) {
	t.Helper()
	record, err := client.Get(t.Context(), addr, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get missing wire value: %v", err)
	}
	if record.Found {
		t.Fatalf("record should be missing: %+v", record)
	}
}
