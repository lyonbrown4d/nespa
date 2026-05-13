package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/internal/controlapi"
)

type ControlClient struct {
	baseURL string
	client  *http.Client
}

func NewControlClient(addr string) *ControlClient {
	return &ControlClient{
		baseURL: normalizeBaseURL(addr),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *ControlClient) Snapshot(ctx context.Context) (controlapi.SnapshotBody, error) {
	var zero controlapi.SnapshotBody
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/control/snapshot", nil)
	if err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "invalid control-plane request", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "control-plane request failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return zero, httpx.NewError(resp.StatusCode, "control-plane snapshot failed")
	}
	if err := json.NewDecoder(resp.Body).Decode(&zero); err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "decode control-plane snapshot", err)
	}
	return zero, nil
}

func hasAddress(addr string) bool {
	return strings.TrimSpace(addr) != ""
}
