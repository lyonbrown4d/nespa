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
	BatchSet(context.Context, cachewire.BatchSetRequest) (cachewire.BatchSetResponse, error)
	BatchGet(context.Context, cachewire.BatchGetRequest) (cachewire.BatchGetResponse, error)
}

type refreshable interface {
	Refresh(context.Context) error
}

func NewDirect(addr string) (*Client, error) {
	backend, err := coreclient.NewTCP(coreclient.Config{Addr: addr})
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
	return backend.Refresh(ctx)
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
		return Record{}, err
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
		return Record{}, err
	}
	return recordFromWire(record), nil
}

func (c *Client) Delete(ctx context.Context, key Key, opts DeleteOptions) (bool, error) {
	response, err := c.backend.Delete(ctx, cachewire.DeleteRequest{
		Key:             wireKey(key),
		ExpectedVersion: opts.ExpectedVersion,
	})
	if err != nil {
		return false, err
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
		return false, err
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
		return false, err
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
		return Record{}, err
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
		return nil, err
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
		return nil, err
	}
	return recordsFromWire(response.Records), nil
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
	for _, record := range records {
		out = append(out, recordFromWire(record))
	}
	return out
}
