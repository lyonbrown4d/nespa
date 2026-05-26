package control_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
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

func TestDragonboatRuntimeBootstrapWithInitialMembers(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	addr1 := freeRaftAddr(t)
	addr2 := freeRaftAddr(t)
	addr3 := freeRaftAddr(t)

	first := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            addr1,
			ClusterID:       103,
			NodeID:          1,
			Members:         []control.RaftMember{{NodeID: 2, Addr: addr2}, {NodeID: 3, Addr: addr3}},
			ProposalTimeout: 15 * time.Second,
		},
	})
	second := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            addr2,
			ClusterID:       103,
			NodeID:          2,
			Members:         []control.RaftMember{{NodeID: 1, Addr: addr1}, {NodeID: 3, Addr: addr3}},
			ProposalTimeout: 15 * time.Second,
		},
	})
	third := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            addr3,
			ClusterID:       103,
			NodeID:          3,
			Members:         []control.RaftMember{{NodeID: 1, Addr: addr1}, {NodeID: 2, Addr: addr2}},
			ProposalTimeout: 15 * time.Second,
		},
	})

	startDragonboatRuntimes(t, logger, first, second, third)
	t.Cleanup(func() {
		stopDragonboatRuntimes(t, logger, third, second, first)
	})

	members := waitForRaftMemberCount(t, first, 3)
	for nodeID, expectedAddr := range map[uint64]string{
		1: addr1,
		2: addr2,
		3: addr3,
	} {
		if got, ok := members[nodeID]; !ok {
			t.Fatalf("raft members missing node=%d, got=%+v", nodeID, members)
		} else if got != expectedAddr {
			t.Fatalf("raft member addr mismatch node=%d got=%s want=%s", nodeID, got, expectedAddr)
		}
	}

	if _, err := first.CreateNamespace(t.Context(), "orders"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
}

func TestDragonboatRuntimeMembershipAddRemove(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	addr2 := freeRaftAddr(t)
	svc := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            freeRaftAddr(t),
			ClusterID:       104,
			NodeID:          1,
			ProposalTimeout: 10 * time.Second,
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

	waitForRaftMemberCount(t, svc, 1)

	if err := svc.AddRaftNode(t.Context(), 2, addr2); err != nil {
		t.Fatalf("add raft node: %v", err)
	}

	joined := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            addr2,
			ClusterID:       104,
			NodeID:          2,
			Join:            true,
			ProposalTimeout: 10 * time.Second,
		},
	})
	if err := control.StartDragonboat(t.Context(), logger, joined); err != nil {
		t.Fatalf("start joined raft node: %v", err)
	}
	t.Cleanup(func() {
		if err := control.StopDragonboat(context.Background(), logger, joined); err != nil {
			t.Errorf("stop joined dragonboat: %v", err)
		}
	})
	waitForRaftMemberCount(t, svc, 2)

	if err := svc.RemoveRaftNode(t.Context(), 2); err != nil {
		t.Fatalf("remove raft node: %v", err)
	}
	waitForRaftMemberCount(t, svc, 1)
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

func startDragonboatRuntimes(t *testing.T, logger *slog.Logger, runtimes ...*control.ServiceRuntime) {
	t.Helper()

	var wg sync.WaitGroup
	errs := make(chan error, len(runtimes))
	for _, runtime := range runtimes {
		wg.Go(func() {
			errs <- control.StartDragonboat(t.Context(), logger, runtime)
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			stopDragonboatRuntimes(t, logger, runtimes...)
			t.Fatalf("start dragonboat runtime: %v", err)
		}
	}
}

func stopDragonboatRuntimes(t *testing.T, logger *slog.Logger, runtimes ...*control.ServiceRuntime) {
	t.Helper()
	for _, runtime := range runtimes {
		if err := control.StopDragonboat(context.Background(), logger, runtime); err != nil {
			t.Errorf("stop dragonboat: %v", err)
		}
	}
}

func waitForRaftMemberCount(t *testing.T, svc *control.ServiceRuntime, want int) map[uint64]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		members, err := svc.RaftMembers(t.Context())
		if err == nil && len(members) == want {
			return members
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("raft members=%+v want count=%d", getRaftMembers(t, svc), want)
	return nil
}

func getRaftMembers(t *testing.T, svc *control.ServiceRuntime) map[uint64]string {
	t.Helper()
	members, err := svc.RaftMembers(t.Context())
	if err != nil {
		t.Fatalf("fetch raft members: %v", err)
	}
	return members
}
