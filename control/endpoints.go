package control

import (
	"context"

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
	state *ControlState
}

type nodeEndpoint struct {
	state *ControlState
}

func NewReadEndpoint(svc *ServiceRuntime) Endpoint {
	return &readEndpoint{state: svc.state}
}

func NewCatalogEndpoint(svc *ServiceRuntime) Endpoint {
	return &catalogEndpoint{state: svc.state}
}

func NewNodeEndpoint(svc *ServiceRuntime) Endpoint {
	return &nodeEndpoint{state: svc.state}
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
	return runtime.JSON(e.state.Namespaces()), nil
}

func (e *catalogEndpoint) CreateNamespace(
	_ context.Context,
	input *controlapi.CreateNamespaceInput,
) (*runtime.JSONResponse[controlapi.CreateNamespaceResponse], error) {
	response, err := e.state.CreateNamespace(input.Body.Namespace)
	if err != nil {
		return nil, controlStateError("create namespace failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) BumpNamespaceVersion(
	_ context.Context,
	input *controlapi.BumpNamespaceVersionInput,
) (*runtime.JSONResponse[controlapi.BumpNamespaceVersionResponse], error) {
	response, err := e.state.BumpNamespaceVersion(input.Body.Namespace)
	if err != nil {
		return nil, controlStateError("bump namespace version failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) Spaces(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.SpacesBody], error) {
	return runtime.JSON(e.state.Spaces()), nil
}

func (e *catalogEndpoint) CreateSpace(
	ctx context.Context,
	input *controlapi.CreateSpaceInput,
) (*runtime.JSONResponse[controlapi.CreateSpaceResponse], error) {
	response, err := e.state.CreateSpace(ctx, input.Body.Namespace, input.Body.Space)
	if err != nil {
		return nil, controlStateError("create space failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) BumpSpaceVersion(
	_ context.Context,
	input *controlapi.BumpSpaceVersionInput,
) (*runtime.JSONResponse[controlapi.BumpSpaceVersionResponse], error) {
	response, err := e.state.BumpSpaceVersion(input.Body.Namespace, input.Body.Space)
	if err != nil {
		return nil, controlStateError("bump space version failed", err)
	}
	return runtime.JSON(response), nil
}

func (e *catalogEndpoint) Entities(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.EntitiesBody], error) {
	return runtime.JSON(e.state.Entities()), nil
}

func (e *catalogEndpoint) CreateEntity(
	_ context.Context,
	input *controlapi.CreateEntityInput,
) (*runtime.JSONResponse[controlapi.CreateEntityResponse], error) {
	response, err := e.state.CreateEntity(input.Body.Namespace, input.Body.Space, input.Body.Entity)
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
	httpx.MustGroupPut(scope, "/nodes/heartbeat", e.Heartbeat)
}

func (e *nodeEndpoint) Nodes(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[controlapi.NodesBody], error) {
	return runtime.JSON(e.state.Nodes()), nil
}

func (e *nodeEndpoint) RegisterNode(
	ctx context.Context,
	input *controlapi.RegisterNodeInput,
) (*runtime.JSONResponse[controlapi.RegisterNodeResponse], error) {
	response, err := e.state.RegisterNode(ctx, input.Body.NodeID, input.Body.Addr)
	if err != nil {
		return nil, controlStateError("invalid node registration", err)
	}
	return runtime.JSON(response), nil
}

func (e *nodeEndpoint) Heartbeat(
	ctx context.Context,
	input *controlapi.HeartbeatInput,
) (*runtime.JSONResponse[controlapi.HeartbeatResponse], error) {
	response, err := e.state.Heartbeat(ctx, input.Body.NodeID, input.Body.Addr)
	if err != nil {
		return nil, controlStateError("invalid node heartbeat", err)
	}
	return runtime.JSON(response), nil
}

func controlEndpointSpec() httpx.EndpointSpec {
	return httpx.EndpointSpec{
		Prefix: "/v1/control",
	}
}
