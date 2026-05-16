package nespa

import (
	"context"
	"errors"
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
	coreclient "github.com/lyonbrown4d/nespa/client"
)

type Client struct {
	backend rawClient
}

type rawClient interface {
	Set(context.Context, cachewire.SetRequest) (cachewire.Record, error)
	Get(context.Context, cachewire.GetRequest) (cachewire.Record, error)
	Delete(context.Context, cachewire.DeleteRequest) (cachewire.DeleteResponse, error)
	Exists(context.Context, cachewire.ExistsRequest) (cachewire.ExistsResponse, error)
	Touch(context.Context, cachewire.TouchRequest) (cachewire.TouchResponse, error)
	Adjust(context.Context, cachewire.AdjustRequest) (cachewire.Record, error)
	Primitive(context.Context, cachewire.PrimitiveRequest) (cachewire.PrimitiveResult, error)
	BatchSet(context.Context, cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error)
	BatchGet(context.Context, cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error)
	BatchDelete(context.Context, cachewire.BatchDeleteRequest) (cachewire.BatchDeleteResponse, error)
	BatchExists(context.Context, cachewire.BatchExistsRequest) (cachewire.BatchExistsResponse, error)
	BatchTouch(context.Context, cachewire.BatchTouchRequest) (cachewire.BatchTouchResponse, error)
	BatchPrimitive(context.Context, cachewire.BatchPrimitiveRequest) (cachewire.BatchPrimitiveResponse, error)
}

type refreshable interface {
	Refresh(context.Context) error
}

func NewDirect(addr string) (*Client, error) {
	backend, err := newDirectTCPClient(addr)
	if err != nil {
		return nil, fmt.Errorf("create direct nespa client: %w", err)
	}
	return &Client{backend: backend}, nil
}

func NewRouted(controlAddr string) (*Client, error) {
	backend, err := coreclient.NewRoutedTCP(coreclient.RoutedConfig{ControlAddr: controlAddr})
	if err != nil {
		return nil, fmt.Errorf("create routed nespa client: %w", err)
	}
	return &Client{backend: backend}, nil
}

func (c *Client) Refresh(ctx context.Context) error {
	backend, ok := c.backend.(refreshable)
	if !ok {
		return nil
	}
	if err := backend.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh nespa client: %w", err)
	}
	return nil
}

func (c *Client) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (Record, error) {
	record, err := c.backend.Set(ctx, cachewire.SetRequest{
		Key:              wireKey(key),
		Value:            append([]byte(nil), value...),
		TTLMillis:        ttlMillis(opts.TTL),
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return Record{}, fmt.Errorf("set nespa record: %w", err)
	}
	return recordFromWire(record), nil
}

func (c *Client) Get(ctx context.Context, key Key, opts GetOptions) (Record, error) {
	record, err := c.backend.Get(ctx, cachewire.GetRequest{
		Key:              wireKey(key),
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
	if err != nil {
		return Record{}, fmt.Errorf("get nespa record: %w", err)
	}
	return recordFromWire(record), nil
}

func (c *Client) Delete(ctx context.Context, key Key, opts DeleteOptions) (bool, error) {
	response, err := c.backend.Delete(ctx, cachewire.DeleteRequest{
		Key:             wireKey(key),
		ExpectedVersion: opts.ExpectedVersion,
	})
	if err != nil {
		return false, fmt.Errorf("delete nespa record: %w", err)
	}
	return response.Deleted, nil
}

func (c *Client) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	response, err := c.backend.Exists(ctx, cachewire.ExistsRequest{
		Key:              wireKey(key),
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
	})
	if err != nil {
		return false, fmt.Errorf("check nespa record: %w", err)
	}
	return response.Exists, nil
}

func (c *Client) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	response, err := c.backend.Touch(ctx, cachewire.TouchRequest{
		Key:              wireKey(key),
		TTLMillis:        ttlMillis(opts.TTL),
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return false, fmt.Errorf("touch nespa record: %w", err)
	}
	return response.Touched, nil
}

func (c *Client) Adjust(ctx context.Context, key Key, opts AdjustOptions) (Record, error) {
	record, err := c.backend.Adjust(ctx, cachewire.AdjustRequest{
		Key:              wireKey(key),
		TTLMillis:        ttlMillis(opts.TTL),
		InitialValue:     opts.InitialValue,
		Delta:            opts.Delta,
		NamespaceVersion: opts.NamespaceVersion,
		SpaceVersion:     opts.SpaceVersion,
		ExpectedVersion:  opts.ExpectedVersion,
	})
	if err != nil {
		return Record{}, fmt.Errorf("adjust nespa record: %w", err)
	}
	return recordFromWire(record), nil
}

func (c *Client) BatchSet(ctx context.Context, items []SetItem) ([]Record, error) {
	requests := make([]cachewire.SetRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cachewire.SetRequest{
			Key:              wireKey(item.Key),
			Value:            append([]byte(nil), item.Value...),
			TTLMillis:        ttlMillis(item.Options.TTL),
			NamespaceVersion: item.Options.NamespaceVersion,
			SpaceVersion:     item.Options.SpaceVersion,
			ExpectedVersion:  item.Options.ExpectedVersion,
		})
	}
	response, err := c.backend.BatchSet(ctx, cachewire.BatchSetRequest{Items: requests})
	if err != nil {
		return nil, fmt.Errorf("batch set nespa records: %w", err)
	}
	return recordsFromWire(response.Records), nil
}

func (c *Client) BatchGet(ctx context.Context, items []GetItem) ([]Record, error) {
	requests := make([]cachewire.GetRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cachewire.GetRequest{
			Key:              wireKey(item.Key),
			NamespaceVersion: item.Options.NamespaceVersion,
			SpaceVersion:     item.Options.SpaceVersion,
		})
	}
	response, err := c.backend.BatchGet(ctx, cachewire.BatchGetRequest{Items: requests})
	if err != nil {
		return nil, fmt.Errorf("batch get nespa records: %w", err)
	}
	return recordsFromWire(response.Records), nil
}

func (c *Client) BatchDelete(ctx context.Context, items []DeleteItem) ([]bool, error) {
	requests := make([]cachewire.DeleteRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cachewire.DeleteRequest{
			Key:             wireKey(item.Key),
			ExpectedVersion: item.Options.ExpectedVersion,
		})
	}
	response, err := c.backend.BatchDelete(ctx, cachewire.BatchDeleteRequest{Items: requests})
	if err != nil {
		return nil, fmt.Errorf("batch delete nespa records: %w", err)
	}
	return deleteResultsFromWire(response.Results), nil
}

func (c *Client) BatchExists(ctx context.Context, items []GetItem) ([]bool, error) {
	requests := make([]cachewire.ExistsRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cachewire.ExistsRequest{
			Key:              wireKey(item.Key),
			NamespaceVersion: item.Options.NamespaceVersion,
			SpaceVersion:     item.Options.SpaceVersion,
		})
	}
	response, err := c.backend.BatchExists(ctx, cachewire.BatchExistsRequest{Items: requests})
	if err != nil {
		return nil, fmt.Errorf("batch check nespa records: %w", err)
	}
	return existsResultsFromWire(response.Results), nil
}

func (c *Client) BatchTouch(ctx context.Context, items []TouchItem) ([]bool, error) {
	requests := make([]cachewire.TouchRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cachewire.TouchRequest{
			Key:              wireKey(item.Key),
			TTLMillis:        ttlMillis(item.Options.TTL),
			NamespaceVersion: item.Options.NamespaceVersion,
			SpaceVersion:     item.Options.SpaceVersion,
			ExpectedVersion:  item.Options.ExpectedVersion,
		})
	}
	response, err := c.backend.BatchTouch(ctx, cachewire.BatchTouchRequest{Items: requests})
	if err != nil {
		return nil, fmt.Errorf("batch touch nespa records: %w", err)
	}
	return touchResultsFromWire(response.Results), nil
}

func ErrorCodeOf(err error) (ErrorCode, bool) {
	var wireErr cachewire.Error
	if errors.As(err, &wireErr) {
		return wireErr.Code, true
	}
	return 0, false
}

func recordsFromWire(records []cachewire.Record) []Record {
	out := make([]Record, 0, len(records))
	for index := range records {
		out = append(out, recordFromWire(records[index]))
	}
	return out
}

func deleteResultsFromWire(results []cachewire.DeleteResponse) []bool {
	out := make([]bool, 0, len(results))
	for index := range results {
		out = append(out, results[index].Deleted)
	}
	return out
}

func existsResultsFromWire(results []cachewire.ExistsResponse) []bool {
	out := make([]bool, 0, len(results))
	for index := range results {
		out = append(out, results[index].Exists)
	}
	return out
}

func touchResultsFromWire(results []cachewire.TouchResponse) []bool {
	out := make([]bool, 0, len(results))
	for index := range results {
		out = append(out, results[index].Touched)
	}
	return out
}
