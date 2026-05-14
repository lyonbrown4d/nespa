package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func jsonFrame(request protocol.Frame, metadata any, payload []byte) protocol.Frame {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return errorFrame(request, protocol.ErrorInternal, err)
	}
	return protocol.Frame{
		Flags:      protocol.FlagResponse,
		Op:         request.Op,
		RequestID:  request.RequestID,
		RouteEpoch: request.RouteEpoch,
		Metadata:   raw,
		Payload:    payload,
	}
}

func cacheErrorFrame(request protocol.Frame, err error) protocol.Frame {
	switch {
	case errors.Is(err, cache.ErrQuotaExceeded):
		return errorFrame(request, protocol.ErrorTooLarge, err)
	case errors.Is(err, engine.ErrInvalidKey):
		return errorFrame(request, protocol.ErrorBadFrame, err)
	case errors.Is(err, context.DeadlineExceeded):
		return errorFrame(request, protocol.ErrorTimeout, err)
	case errors.Is(err, context.Canceled):
		return errorFrame(request, protocol.ErrorUnavailable, err)
	default:
		return errorFrame(request, protocol.ErrorInternal, err)
	}
}

func errorFrame(request protocol.Frame, code protocol.ErrorCode, err error) protocol.Frame {
	raw, marshalErr := json.Marshal(cachewire.Error{Code: code, Message: err.Error()})
	if marshalErr != nil {
		raw = []byte(`{"code":8,"message":"cache tcp error"}`)
	}
	return protocol.Frame{
		Flags:      protocol.FlagResponse | protocol.FlagError,
		Op:         request.Op,
		RequestID:  request.RequestID,
		RouteEpoch: request.RouteEpoch,
		Metadata:   raw,
	}
}

func keyFromWire(key cachewire.Key) cache.Key {
	return cache.Key{Namespace: key.Namespace, Space: key.Space, Entity: key.Entity, Key: key.Key}
}

func batchSetRequests(items []cachewire.SetRequest) []cache.SetRequest {
	requests := make([]cache.SetRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cache.SetRequest{
			Key:   keyFromWire(item.Key),
			Value: item.Value,
			Options: cache.SetOptions{
				TTL:              ttlFromMillis(item.TTLMillis),
				NamespaceVersion: item.NamespaceVersion,
				SpaceVersion:     item.SpaceVersion,
			},
		})
	}
	return requests
}

func batchGetRequests(items []cachewire.GetRequest) []cache.GetRequest {
	requests := make([]cache.GetRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, cache.GetRequest{
			Key: keyFromWire(item.Key),
			Options: cache.GetOptions{
				NamespaceVersion: item.NamespaceVersion,
				SpaceVersion:     item.SpaceVersion,
			},
		})
	}
	return requests
}

func recordsFromCache(records []cache.Record) []cachewire.Record {
	out := make([]cachewire.Record, 0, len(records))
	for index := range records {
		out = append(out, recordFromCache(records[index], true))
	}
	return out
}

func recordsFromResults(results []cache.GetResult) []cachewire.Record {
	out := make([]cachewire.Record, 0, len(results))
	for index := range results {
		result := results[index]
		out = append(out, recordFromCache(result.Record, result.Found))
	}
	return out
}

func recordFromCache(rec cache.Record, found bool) cachewire.Record {
	if !found {
		return cachewire.Record{Found: false}
	}
	out := cachewire.Record{
		Found:            true,
		Namespace:        rec.Key.Namespace,
		Space:            rec.Key.Space,
		Entity:           rec.Key.Entity,
		Key:              rec.Key.Key,
		Value:            rec.Value,
		Version:          rec.Version,
		NamespaceVersion: rec.NamespaceVersion,
		SpaceVersion:     rec.SpaceVersion,
	}
	if !rec.ExpireAt.IsZero() {
		out.ExpireAtUnixMs = rec.ExpireAt.UnixMilli()
	}
	return out
}

func ttlFromMillis(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}
