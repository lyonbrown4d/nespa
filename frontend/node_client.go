package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/lyonbrown4d/nespa/cacheapi"
)

const nodeCachePath = "/v1/node/cache"

type NodeClient struct {
	client *http.Client
}

func NewNodeClient() *NodeClient {
	return &NodeClient{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *NodeClient) Set(ctx context.Context, addr string, body cacheapi.SetBody) (cacheapi.RecordBody, error) {
	return doJSON[cacheapi.RecordBody](ctx, c.client, http.MethodPut, normalizeBaseURL(addr)+nodeCachePath, body)
}

func (c *NodeClient) Get(ctx context.Context, addr string, input cacheapi.GetInput) (cacheapi.RecordBody, error) {
	return doJSON[cacheapi.RecordBody](ctx, c.client, http.MethodGet, normalizeBaseURL(addr)+nodeCachePath+"?"+cacheGetQuery(input).Encode(), nil)
}

func (c *NodeClient) Delete(ctx context.Context, addr string, input cacheapi.DeleteInput) (cacheapi.DeleteBody, error) {
	return doJSON[cacheapi.DeleteBody](ctx, c.client, http.MethodDelete, normalizeBaseURL(addr)+nodeCachePath+"?"+cacheDeleteQuery(input).Encode(), nil)
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

func cacheGetQuery(input cacheapi.GetInput) url.Values {
	values := cacheDeleteQuery(cacheapi.DeleteInput{
		Namespace: input.Namespace,
		Space:     input.Space,
		Entity:    input.Entity,
		Key:       input.Key,
	})
	if input.NamespaceVersion != 0 {
		values.Set("namespace_version", strconv.FormatUint(input.NamespaceVersion, 10))
	}
	if input.SpaceVersion != 0 {
		values.Set("space_version", strconv.FormatUint(input.SpaceVersion, 10))
	}
	return values
}

func cacheDeleteQuery(input cacheapi.DeleteInput) url.Values {
	values := make(url.Values)
	values.Set("namespace", input.Namespace)
	values.Set("space", input.Space)
	if input.Entity != "" {
		values.Set("entity", input.Entity)
	}
	values.Set("key", input.Key)
	return values
}

func doJSON[T any](ctx context.Context, client *http.Client, method, target string, body any) (out T, err error) {
	reader, err := requestBody(body)
	if err != nil {
		return out, err
	}

	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "invalid data-node request", err)
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "data-node request failed", err)
	}
	defer func() {
		err = closeResponseBody(resp.Body, err)
	}()

	if err := checkNodeResponse(resp); err != nil {
		return out, err
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, httpx.NewError(http.StatusBadGateway, "decode data-node response", err)
	}
	return out, nil
}

func requestBody(body any) (io.Reader, error) {
	if body == nil {
		return http.NoBody, nil
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode node request: %w", err)
	}
	return bytes.NewReader(payload), nil
}

func checkNodeResponse(resp *http.Response) error {
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return httpx.NewError(resp.StatusCode, "read data-node error response", err)
	}
	return httpx.NewError(resp.StatusCode, "data-node request failed", fmt.Errorf("%s", strings.TrimSpace(string(raw))))
}
