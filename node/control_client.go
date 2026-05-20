// Package node implements the Nespa data-node service.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/controlapi"
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

func (c *ControlClient) RegisterNode(ctx context.Context, body controlapi.RegisterNodeBody) (controlapi.RegisterNodeResponse, error) {
	return controlDoJSON[controlapi.RegisterNodeResponse](ctx, c.client, http.MethodPost, c.baseURL+"/v1/control/nodes", body)
}

func (c *ControlClient) Heartbeat(ctx context.Context, body controlapi.HeartbeatBody) (controlapi.HeartbeatResponse, error) {
	return controlDoJSON[controlapi.HeartbeatResponse](ctx, c.client, http.MethodPut, c.baseURL+"/v1/control/nodes/heartbeat", body)
}

func (c *ControlClient) Snapshot(ctx context.Context) (controlapi.SnapshotBody, error) {
	return controlGetJSON[controlapi.SnapshotBody](ctx, c.client, c.baseURL+"/v1/control/snapshot")
}

func normalizeBaseURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}

func controlDoJSON[T any](ctx context.Context, client *http.Client, method, target string, body any) (out T, err error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return out, fmt.Errorf("encode control request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "invalid control-plane request", err)
	}
	req.Header.Set("content-type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "control-plane request failed", err)
	}
	defer func() {
		err = closeResponseBody(resp.Body, err)
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return out, httpx.NewError(resp.StatusCode, "read control-plane error response", readErr)
		}
		return out, httpx.NewError(resp.StatusCode, "control-plane request failed", fmt.Errorf("%s", strings.TrimSpace(string(raw))))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "decode control-plane response", err)
	}
	return out, nil
}

func controlGetJSON[T any](ctx context.Context, client *http.Client, target string) (out T, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "invalid control-plane request", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "control-plane request failed", err)
	}
	defer func() {
		err = closeResponseBody(resp.Body, err)
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return out, httpx.NewError(resp.StatusCode, "read control-plane error response", readErr)
		}
		return out, httpx.NewError(resp.StatusCode, "control-plane request failed", fmt.Errorf("%s", strings.TrimSpace(string(raw))))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "decode control-plane response", err)
	}
	return out, nil
}

func closeResponseBody(body io.Closer, err error) error {
	if closeErr := body.Close(); closeErr != nil && err == nil {
		return fmt.Errorf("close response body: %w", closeErr)
	}
	return err
}
