package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	"github.com/samber/oops"
)

const (
	oopsCodeQuotaExceeded      = "quota_exceeded"
	oopsCodeInvalidKey         = "invalid_key"
	oopsCodeInvalidCounter     = "invalid_counter"
	oopsCodeInvalidCounterText = "invalid_counter_value"
	oopsCodeCounterOverflow    = "counter_overflow"
)

func metadataFrame(request protocol.Frame, metadata, payload []byte) protocol.Frame {
	return protocol.Frame{
		Flags:      protocol.FlagResponse,
		Op:         request.Op,
		RequestID:  request.RequestID,
		RouteEpoch: request.RouteEpoch,
		Metadata:   metadata,
		Payload:    payload,
	}
}

func cacheErrorFrame(request protocol.Frame, err error) protocol.Frame {
	switch {
	case hasOopsCode(err, oopsCodeInvalidKey):
		return errorFrame(request, protocol.ErrorBadFrame, err)
	case hasOopsCode(err, oopsCodeInvalidCounter, oopsCodeInvalidCounterText, oopsCodeCounterOverflow):
		return errorFrame(request, protocol.ErrorInvalidArgument, err)
	case hasOopsCode(err, oopsCodeQuotaExceeded):
		return errorFrame(request, protocol.ErrorTooLarge, err)
	case errors.Is(err, context.DeadlineExceeded):
		return errorFrame(request, protocol.ErrorTimeout, err)
	case errors.Is(err, context.Canceled):
		return errorFrame(request, protocol.ErrorUnavailable, err)
	default:
		return errorFrame(request, protocol.ErrorInternal, err)
	}
}

func hasOopsCode(err error, codes ...string) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		oopsErr, ok := oops.AsOops(current)
		if !ok {
			continue
		}
		code, ok := oopsErr.Code().(string)
		if !ok {
			continue
		}
		if slices.Contains(codes, code) {
			return true
		}
	}
	return false
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
	for index := range items {
		item := items[index]
		requests = append(requests, cache.SetRequest{
			Key:   keyFromWire(item.Key),
			Value: item.Value,
			Options: cache.SetOptions{
				TTL:              ttlFromMillis(item.TTLMillis),
				NamespaceVersion: item.NamespaceVersion,
				SpaceVersion:     item.SpaceVersion,
				ExpectedVersion:  item.ExpectedVersion,
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

func getOptionsFromExists(request cachewire.ExistsRequest) cache.GetOptions {
	return cache.GetOptions{
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
	}
}

func touchOptionsFromWire(request cachewire.TouchRequest) cache.TouchOptions {
	return cache.TouchOptions{
		TTL:              ttlFromMillis(request.TTLMillis),
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
		ExpectedVersion:  request.ExpectedVersion,
	}
}

func adjustOptionsFromWire(request cachewire.AdjustRequest) cache.AdjustOptions {
	return cache.AdjustOptions{
		TTL:              ttlFromMillis(request.TTLMillis),
		InitialValue:     request.InitialValue,
		Delta:            request.Delta,
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
		ExpectedVersion:  request.ExpectedVersion,
	}
}

func recordsFromResults(results []cache.GetResult) []cachewire.Record {
	out := make([]cachewire.Record, 0, len(results))
	for index := range results {
		result := results[index]
		out = append(out, recordFromCache(result.Record, result.Found))
	}
	return out
}

func recordsFromSetResults(results []cache.SetResult) []cachewire.Record {
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
