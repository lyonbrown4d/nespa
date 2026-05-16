package tcp_test

import (
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
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

func migrationRangeForKey(key cachewire.Key) cachewire.MigrationRangeRequest {
	slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
	return cachewire.MigrationRangeRequest{
		Namespace:  key.Namespace,
		Space:      key.Space,
		VSlotStart: slot,
		VSlotEnd:   slot,
	}
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
