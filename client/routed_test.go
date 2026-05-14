package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	cachetcp "github.com/lyonbrown4d/nespa/transport/tcp"
)

func TestRoutedTCPClientUsesControlSnapshotVersions(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	data := cachetcp.NewServer(cachetcp.ServerConfig{Addr: "127.0.0.1:0"}, cache.NewService(eng))
	ctx := context.Background()
	if err := data.Start(ctx, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("start tcp server: %v", err)
	}
	defer stopServer(t, data)

	control := newSnapshotServer(t, snapshotFor(data.Addr(), 1, 1))
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}

	key := cachewire.Key{Namespace: "orders", Space: "session", Key: "k"}
	if _, setErr := routed.Set(ctx, cachewire.SetRequest{Key: key, Value: []byte("v1")}); setErr != nil {
		t.Fatalf("set v1: %v", setErr)
	}
	requireValue(t, routed, key, "v1", 1, 1)

	control.Set(snapshotFor(data.Addr(), 2, 2))
	if refreshErr := routed.Refresh(ctx); refreshErr != nil {
		t.Fatalf("refresh snapshot: %v", refreshErr)
	}
	miss, err := routed.Get(ctx, cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get after version bump: %v", err)
	}
	if miss.Found {
		t.Fatalf("record found after version bump: %+v", miss)
	}

	if _, err := routed.Set(ctx, cachewire.SetRequest{Key: key, Value: []byte("v2")}); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	requireValue(t, routed, key, "v2", 2, 2)
}

func TestRoutedTCPClientReportsMissingRoute(t *testing.T) {
	control := newSnapshotServer(t, controlapi.SnapshotBody{Revision: 1})
	defer control.Close()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: control.URL})
	if err != nil {
		t.Fatalf("new routed client: %v", err)
	}

	_, err = routed.Get(context.Background(), cachewire.GetRequest{
		Key: cachewire.Key{Namespace: "orders", Space: "session", Key: "k"},
	})
	if !errors.Is(err, client.ErrNoRoute) {
		t.Fatalf("err = %v, want ErrNoRoute", err)
	}
}

type snapshotServer struct {
	*httptest.Server
	mu       sync.Mutex
	snapshot controlapi.SnapshotBody
}

func newSnapshotServer(t *testing.T, initial controlapi.SnapshotBody) *snapshotServer {
	t.Helper()
	out := &snapshotServer{snapshot: initial}
	out.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/control/snapshot" {
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
		out.mu.Lock()
		defer out.mu.Unlock()

		w.Header().Set("content-type", "application/json")
		if err := json.NewEncoder(w).Encode(out.snapshot); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}
	}))
	return out
}

func (s *snapshotServer) Set(snapshot controlapi.SnapshotBody) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
}

func snapshotFor(addr string, revision, version uint64) controlapi.SnapshotBody {
	return controlapi.SnapshotBody{
		Revision: revision,
		Namespaces: []controlapi.NamespaceBody{
			{Namespace: "orders", Version: version},
		},
		Spaces: []controlapi.SpaceBody{
			{Namespace: "orders", Space: "session", Version: version},
		},
		Routes: []controlapi.RouteBody{
			{Namespace: "orders", Space: "session", VSlotStart: 0, VSlotEnd: controlapi.VSlotMax, NodeID: "node-1", Addr: addr, Weight: 1},
		},
	}
}

func requireValue(t *testing.T, routed *client.RoutedTCPClient, key cachewire.Key, want string, namespaceVersion, spaceVersion uint64) {
	t.Helper()
	record, err := routed.Get(context.Background(), cachewire.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("get value: %v", err)
	}
	if !record.Found || string(record.Value) != want {
		t.Fatalf("record = %+v, want value %q", record, want)
	}
	if record.NamespaceVersion != namespaceVersion || record.SpaceVersion != spaceVersion {
		t.Fatalf("record versions = %d/%d, want %d/%d", record.NamespaceVersion, record.SpaceVersion, namespaceVersion, spaceVersion)
	}
}

func closeEngine(t *testing.T, eng *engine.MemoryEngine) {
	t.Helper()
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func stopServer(t *testing.T, server *cachetcp.Server) {
	t.Helper()
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("stop tcp server: %v", err)
	}
}
