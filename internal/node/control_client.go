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

func (c *ControlClient) RegisterNode(ctx context.Context, body controlapi.RegisterNodeBody) (controlapi.RegisterNodeResponse, error) {
	return controlDoJSON[controlapi.RegisterNodeResponse](ctx, c.client, http.MethodPost, c.baseURL+"/v1/control/nodes", body)
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

func controlDoJSON[T any](ctx context.Context, client *http.Client, method, target string, body any) (T, error) {
	var zero T
	payload, err := json.Marshal(body)
	if err != nil {
		return zero, fmt.Errorf("encode control request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
	if err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "invalid control-plane request", err)
	}
	req.Header.Set("content-type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "control-plane request failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return zero, httpx.NewError(resp.StatusCode, "control-plane request failed", fmt.Errorf("%s", strings.TrimSpace(string(raw))))
	}
	if err := json.NewDecoder(resp.Body).Decode(&zero); err != nil {
		return zero, httpx.NewError(http.StatusBadGateway, "decode control-plane response", err)
	}
	return zero, nil
}
