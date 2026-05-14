package admin

import (
	"context"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/runtime"
)

type Endpoint interface {
	httpx.Endpoint
	adminEndpoint()
}

type summaryEndpoint struct {
	cfg Config
}

func NewSummaryEndpoint(cfg Config) Endpoint {
	return &summaryEndpoint{cfg: cfg}
}

func (e *summaryEndpoint) adminEndpoint() {}

func (e *summaryEndpoint) EndpointSpec() httpx.EndpointSpec {
	return httpx.EndpointSpec{
		Prefix: "/v1/admin",
	}
}

func (e *summaryEndpoint) Register(registrar httpx.Registrar) {
	httpx.MustGroupGet(registrar.Scope(), "/summary", e.Summary)
}

func (e *summaryEndpoint) Summary(context.Context, *runtime.EmptyInput) (*runtime.JSONResponse[SummaryBody], error) {
	return runtime.JSON(SummaryBody{
		ControlAddr: e.cfg.ControlAddr,
		Namespaces:  0,
		Spaces:      0,
		Nodes:       0,
	}), nil
}
