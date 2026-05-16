package control_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/control"
)

func TestDragonboatRuntimeAppliesControlCommands(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	svc := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            freeRaftAddr(t),
			ClusterID:       101,
			NodeID:          1,
			ProposalTimeout: 5 * time.Second,
		},
	})

	if err := control.StartDragonboat(t.Context(), logger, svc); err != nil {
		t.Fatalf("start dragonboat: %v", err)
	}
	t.Cleanup(func() {
		if err := control.StopDragonboat(context.Background(), logger, svc); err != nil {
			t.Errorf("stop dragonboat: %v", err)
		}
	})

	if _, err := svc.CreateNamespace(t.Context(), "orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := svc.CreateSpace(t.Context(), "orders", "session"); err != nil {
		t.Fatalf("create space: %v", err)
	}

	spaces := svc.Spaces()
	if spaces.Revision != 2 || len(spaces.Spaces) != 1 || spaces.Spaces[0].Space != "session" {
		t.Fatalf("spaces = %+v", spaces)
	}
}

func TestDragonboatRuntimeReplaysControlLog(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	dir := t.TempDir()
	addr := freeRaftAddr(t)

	first := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     dir,
			Addr:            addr,
			ClusterID:       102,
			NodeID:          1,
			ProposalTimeout: 5 * time.Second,
		},
	})
	if err := control.StartDragonboat(t.Context(), logger, first); err != nil {
		t.Fatalf("start first dragonboat: %v", err)
	}
	if _, err := first.CreateNamespace(t.Context(), "orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := control.StopDragonboat(context.Background(), logger, first); err != nil {
		t.Fatalf("stop first dragonboat: %v", err)
	}

	second := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     dir,
			Addr:            addr,
			ClusterID:       102,
			NodeID:          1,
			ProposalTimeout: 5 * time.Second,
		},
	})
	if err := control.StartDragonboat(t.Context(), logger, second); err != nil {
		t.Fatalf("start second dragonboat: %v", err)
	}
	t.Cleanup(func() {
		if err := control.StopDragonboat(context.Background(), logger, second); err != nil {
			t.Errorf("stop second dragonboat: %v", err)
		}
	})

	namespaces := second.Namespaces()
	if namespaces.Revision != 1 || len(namespaces.Namespaces) != 1 || namespaces.Namespaces[0].Namespace != "orders" {
		t.Fatalf("namespaces after replay = %+v", namespaces)
	}
}

func freeRaftAddr(t *testing.T) string {
	t.Helper()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free raft addr: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Fatalf("close free raft addr listener: %v", err)
		}
	}()
	return listener.Addr().String()
}
