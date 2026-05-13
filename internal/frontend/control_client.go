// Package frontend implements the Nespa request gateway.
package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func (c *ControlClient) Snapshot(ctx context.Context) (out controlapi.SnapshotBody, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/control/snapshot", http.NoBody)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "invalid control-plane request", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "control-plane request failed", err)
	}
	defer func() {
		err = closeResponseBody(resp.Body, err)
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return out, httpx.NewError(resp.StatusCode, "control-plane snapshot failed")
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "decode control-plane snapshot", err)
	}
	return out, nil
}

func hasAddress(addr string) bool {
	return strings.TrimSpace(addr) != ""
}

func closeResponseBody(body io.Closer, err error) error {
	if closeErr := body.Close(); closeErr != nil && err == nil {
		return fmt.Errorf("close response body: %w", closeErr)
	}
	return err
}
