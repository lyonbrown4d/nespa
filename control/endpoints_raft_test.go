package control_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
)

type raftEndpointClient interface {
	RaftMembers(
		context.Context,
		*runtime.EmptyInput,
	) (*runtime.JSONResponse[controlapi.RaftMembersBody], error)
	AddRaftMember(
		context.Context,
		*controlapi.AddRaftMemberInput,
	) (*runtime.JSONResponse[controlapi.AddRaftMemberResponse], error)
	RemoveRaftMember(
		context.Context,
		*controlapi.RemoveRaftMemberInput,
	) (*runtime.JSONResponse[controlapi.RemoveRaftMemberResponse], error)
}

func TestRaftEndpointMembersRequiresDragonboat(t *testing.T) {
	endpoint := newRaftEndpointClient(t, control.NewServiceRuntime(control.Config{ClusterID: "test"}))

	_, err := endpoint.RaftMembers(t.Context(), &runtime.EmptyInput{})
	requireHTTPStatus(t, err, http.StatusServiceUnavailable)
}

func TestRaftEndpointMembersReturnsSortedMembers(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	addr := freeRaftAddr(t)
	svc := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            addr,
			ClusterID:       105,
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

	endpoint := newRaftEndpointClient(t, svc)
	response, err := endpoint.RaftMembers(t.Context(), &runtime.EmptyInput{})
	if err != nil {
		t.Fatalf("read raft members: %v", err)
	}
	if got, want := response.Body.Members, []controlapi.RaftMemberBody{{NodeID: 1, Addr: addr}}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("raft members = %+v, want %+v", got, want)
	}
}

func TestRaftEndpointInvalidAddRequestReturnsBadRequest(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	svc := control.NewServiceRuntime(control.Config{
		ClusterID: "test",
		Raft: control.RaftConfig{
			NodeHostDir:     t.TempDir(),
			Addr:            freeRaftAddr(t),
			ClusterID:       106,
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

	endpoint := newRaftEndpointClient(t, svc)
	_, err := endpoint.AddRaftMember(t.Context(), &controlapi.AddRaftMemberInput{
		Body: controlapi.AddRaftMemberBody{
			NodeID: 2,
			Addr:   "missing-port",
		},
	})
	requireHTTPStatus(t, err, http.StatusBadRequest)
}

func newRaftEndpointClient(t *testing.T, svc *control.ServiceRuntime) raftEndpointClient {
	t.Helper()

	endpoint, ok := control.NewRaftEndpoint(svc).(raftEndpointClient)
	if !ok {
		t.Fatal("raft endpoint does not implement test client interface")
	}
	return endpoint
}

func requireHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected http status %d error", want)
	}
	var httpErr *httpx.Error
	if !errors.As(err, &httpErr) {
		t.Fatalf("error type = %T, want *httpx.Error", err)
	}
	if httpErr.Code != want {
		t.Fatalf("http status = %d, want %d: %v", httpErr.Code, want, err)
	}
}
