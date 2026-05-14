package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
)

type controlSnapshotClient struct {
	baseURL string
	client  *http.Client
}

func newControlSnapshotClient(addr string) (*controlSnapshotClient, error) {
	baseURL := normalizeControlBaseURL(addr)
	if baseURL == "" {
		return nil, ErrInvalidConfig
	}
	return &controlSnapshotClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}, nil
}

func (c *controlSnapshotClient) Snapshot(ctx context.Context) (out controlapi.SnapshotBody, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/control/snapshot", http.NoBody)
	if err != nil {
		return out, fmt.Errorf("create control snapshot request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return out, fmt.Errorf("request control snapshot: %w", err)
	}
	defer func() {
		err = closeBody(resp.Body, err)
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return out, fmt.Errorf("control snapshot status: %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("decode control snapshot: %w", err)
	}
	return out, nil
}

func normalizeControlBaseURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}

func closeBody(body io.Closer, err error) error {
	if closeErr := body.Close(); closeErr != nil && err == nil {
		return fmt.Errorf("close response body: %w", closeErr)
	}
	return err
}
