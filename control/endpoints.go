package control

import (
	"context"
	"sort"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/runtime"
)

type Endpoint interface {
	httpx.Endpoint
	controlEndpoint()
}

type readEndpoint struct {
	state *ControlState
}

type catalogEndpoint struct {
	svc *ServiceRuntime
}

type nodeEndpoint struct {
	svc *ServiceRuntime
}

type raftEndpoint struct {
	svc *ServiceRuntime
}

func NewReadEndpoint(svc *ServiceRuntime) Endpoint {
	return &readEndpoint{state: svc.state}
}

func NewCatalogEndpoint(svc *ServiceRuntime) Endpoint {
	return &catalogEndpoint{svc: svc}
}

func NewNodeEndpoint(svc *ServiceRuntime) Endpoint {
	return &nodeEndpoint{svc: svc}
}

func NewRaftEndpoint(svc *ServiceRuntime) Endpoint {
	return &raftEndpoint{svc: svc}
}

func (e *readEndpoint) controlEndpoint() {}

func (e *readEndpoint) EndpointSpec() httpx.EndpointSpec {
	return controlEndpointSpec()
}

func (e *readEndpoint) Register(registrar httpx.Registrar) {
	scope := registrar.Scope()
	httpx.MustGroupGet(scope, "/state", e.State)
	httpx.MustGroupGet(scope, "/snapshot", e.Snapshot)
	httpx.MustGroupGet(scope, "/rebalance/events", e.RebalanceEvents)
	httpx.MustGroupGet(scope, "/rebalance/plans", e.MigrationPlans)
}

func (e *readEndpoint) State(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.StateBody], error) {
	return runtime.JSON(e.state.State()), nil
}

func (e *readEndpoint) Snapshot(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SnapshotBody], error) {
	return runtime.JSON(e.state.Snapshot()), nil
}

func (e *readEndpoint) RebalanceEvents(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.RebalanceEventsBody], error) {
	return runtime.JSON(e.state.RebalanceEvents()), nil
}

func (e *readEndpoint) MigrationPlans(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.MigrationPlansBody], error) {
	return runtime.JSON(e.state.MigrationPlans()), nil
}

func (e *catalogEndpoint) controlEndpoint() {}

func (e *catalogEndpoint) EndpointSpec() httpx.EndpointSpec {
	return controlEndpointSpec()
}

func (e *catalogEndpoint) Register(registrar httpx.Registrar) {
	scope := registrar.Scope()
	httpx.MustGroupGet(scope, "/namespaces", e.Namespaces)
	httpx.MustGroupPost(scope, "/namespaces", e.CreateNamespace)
	httpx.MustGroupPost(scope, "/namespaces/version-bump", e.BumpNamespaceVersion)
	httpx.MustGroupGet(scope, "/spaces", e.Spaces)
	httpx.MustGroupPost(scope, "/spaces", e.CreateSpace)
	httpx.MustGroupPost(scope, "/spaces/version-bump", e.BumpSpaceVersion)
	httpx.MustGroupGet(scope, "/entities", e.Entities)
	httpx.MustGroupPost(scope, "/entities", e.CreateEntity)
}

func (e *catalogEndpoint) Namespaces(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NamespacesBody], error) {
	return runtime.JSON(e.svc.Namespaces()), nil
}

func (e *catalogEndpoint) CreateNamespace(
	ctx context.Context,
	input *controlapi.CreateNamespaceInput,
) (*runtime.JSONResponse[controlapi.CreateNamespaceResponse], error) {
	response, err := e.svc.CreateNamespace(ctx, input.Body.Namespace)
	if err != nil {
		return nil, controlStateError("create namespace failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) BumpNamespaceVersion(
	ctx context.Context,
	input *controlapi.BumpNamespaceVersionInput,
) (*runtime.JSONResponse[controlapi.BumpNamespaceVersionResponse], error) {
	response, err := e.svc.BumpNamespaceVersion(ctx, input.Body.Namespace)
	if err != nil {
		return nil, controlStateError("bump namespace version failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) Spaces(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SpacesBody], error) {
	return runtime.JSON(e.svc.Spaces()), nil
}

func (e *catalogEndpoint) CreateSpace(
	ctx context.Context,
	input *controlapi.CreateSpaceInput,
) (*runtime.JSONResponse[controlapi.CreateSpaceResponse], error) {
	response, err := e.svc.CreateSpace(ctx, input.Body.Namespace, input.Body.Space)
	if err != nil {
		return nil, controlStateError("create space failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) BumpSpaceVersion(
	ctx context.Context,
	input *controlapi.BumpSpaceVersionInput,
) (*runtime.JSONResponse[controlapi.BumpSpaceVersionResponse], error) {
	response, err := e.svc.BumpSpaceVersion(ctx, input.Body.Namespace, input.Body.Space)
	if err != nil {
		return nil, controlStateError("bump space version failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) Entities(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.EntitiesBody], error) {
	return runtime.JSON(e.svc.Entities()), nil
}

func (e *catalogEndpoint) CreateEntity(
	ctx context.Context,
	input *controlapi.CreateEntityInput,
) (*runtime.JSONResponse[controlapi.CreateEntityResponse], error) {
	response, err := e.svc.CreateEntity(ctx, input.Body.Namespace, input.Body.Space, input.Body.Entity)
	if err != nil {
		return nil, controlStateError("create entity failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *nodeEndpoint) controlEndpoint() {}

func (e *nodeEndpoint) EndpointSpec() httpx.EndpointSpec {
	return controlEndpointSpec()
}

func (e *nodeEndpoint) Register(registrar httpx.Registrar) {
	scope := registrar.Scope()
	httpx.MustGroupGet(scope, "/nodes", e.Nodes)
	httpx.MustGroupPost(scope, "/nodes", e.RegisterNode)
	httpx.MustGroupPost(scope, "/nodes/remove", e.RemoveNode)
	httpx.MustGroupPut(scope, "/nodes/heartbeat", e.Heartbeat)
}

func (e *nodeEndpoint) Nodes(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NodesBody], error) {
	return runtime.JSON(e.svc.Nodes()), nil
}

func (e *nodeEndpoint) RegisterNode(
	ctx context.Context,
	input *controlapi.RegisterNodeInput,
) (*runtime.JSONResponse[controlapi.RegisterNodeResponse], error) {
	response, err := e.svc.RegisterNode(ctx, input.Body.NodeID, input.Body.Addr)
	if err != nil {
		return nil, controlStateError("invalid node registration", err)
	}
	return runtime.JSON(response), nil
}

func (e *nodeEndpoint) Heartbeat(
	ctx context.Context,
	input *controlapi.HeartbeatInput,
) (*runtime.JSONResponse[controlapi.HeartbeatResponse], error) {
	response, err := e.svc.Heartbeat(ctx, input.Body.NodeID, input.Body.Addr)
	if err != nil {
		return nil, controlStateError("invalid node heartbeat", err)
	}
	return runtime.JSON(response), nil
}

func (e *nodeEndpoint) RemoveNode(
	ctx context.Context,
	input *controlapi.RemoveNodeInput,
) (*runtime.JSONResponse[controlapi.RemoveNodeResponse], error) {
	response, err := e.svc.RemoveNode(ctx, input.Body.NodeID)
	if err != nil {
		return nil, controlStateError("invalid remove node request", err)
	}
	return runtime.JSON(response), nil
}

func (e *raftEndpoint) controlEndpoint() {}

func (e *raftEndpoint) EndpointSpec() httpx.EndpointSpec {
	return controlEndpointSpec()
}

func (e *raftEndpoint) Register(registrar httpx.Registrar) {
	scope := registrar.Scope()
	httpx.MustGroupGet(scope, "/raft/members", e.RaftMembers)
	httpx.MustGroupPost(scope, "/raft/members/add", e.AddRaftMember)
	httpx.MustGroupPost(scope, "/raft/members/remove", e.RemoveRaftMember)
}

func (e *raftEndpoint) RaftMembers(
	ctx context.Context,
	_ *runtime.EmptyInput,
) (*runtime.JSONResponse[controlapi.RaftMembersBody], error) {
	members, err := e.svc.RaftMembers(ctx)
	if err != nil {
		return nil, controlStateError("read raft members failed", err)
	}
	return runtime.JSON(controlapi.RaftMembersBody{Members: raftMemberBodies(members)}), nil
}

func (e *raftEndpoint) AddRaftMember(
	ctx context.Context,
	input *controlapi.AddRaftMemberInput,
) (*runtime.JSONResponse[controlapi.AddRaftMemberResponse], error) {
	if err := e.svc.AddRaftNode(ctx, input.Body.NodeID, input.Body.Addr); err != nil {
		return nil, controlStateError("add raft member failed", err)
	}
	members, err := e.svc.RaftMembers(ctx)
	if err != nil {
		return nil, controlStateError("read raft members failed", err)
	}
	return runtime.JSON(controlapi.AddRaftMemberResponse{
		Member: controlapi.RaftMemberBody{
			NodeID: input.Body.NodeID,
			Addr:   input.Body.Addr,
		},
		Members: raftMemberBodies(members),
	}), nil
}

func (e *raftEndpoint) RemoveRaftMember(
	ctx context.Context,
	input *controlapi.RemoveRaftMemberInput,
) (*runtime.JSONResponse[controlapi.RemoveRaftMemberResponse], error) {
	if err := e.svc.RemoveRaftNode(ctx, input.Body.NodeID); err != nil {
		return nil, controlStateError("remove raft member failed", err)
	}
	members, err := e.svc.RaftMembers(ctx)
	if err != nil {
		return nil, controlStateError("read raft members failed", err)
	}
	return runtime.JSON(controlapi.RemoveRaftMemberResponse{
		NodeID:  input.Body.NodeID,
		Members: raftMemberBodies(members),
	}), nil
}

func raftMemberBodies(members map[uint64]string) []controlapi.RaftMemberBody {
	out := make([]controlapi.RaftMemberBody, 0, len(members))
	for nodeID, addr := range members {
		out = append(out, controlapi.RaftMemberBody{
			NodeID: nodeID,
			Addr:   addr,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

func controlEndpointSpec() httpx.EndpointSpec {
	return httpx.EndpointSpec{
		Prefix: "/v1/control",
	}
}
