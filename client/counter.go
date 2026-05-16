package client

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
)

const defaultCounterMaxRetries = 8

type CounterRequest struct {
	// Key to update.
	Key cachewire.Key
	// Delta applied to the current counter value.
	Delta int64
	// InitialValue is used when the key is not present.
	InitialValue int64
	// TTLMillis is applied when the key is created because it is missing.
	TTLMillis int64
	// NamespaceVersion constrains direct-key clients to the requested namespace version.
	NamespaceVersion uint64
	// SpaceVersion constrains direct-key clients to the requested space version.
	SpaceVersion uint64
	// MaxRetries limits CAS retry attempts on version mismatch. Default is 8.
	MaxRetries int
}

type CounterResult struct {
	// Record returned by the successful counter write.
	Record cachewire.Record
	// Value is the new counter value.
	Value int64
}

func (c *TCPClient) Counter(ctx context.Context, request CounterRequest) (CounterResult, error) {
	getter := func(ctx context.Context, get cachewire.GetRequest) (cachewire.Record, error) {
		get.Key = request.Key
		get.NamespaceVersion = request.NamespaceVersion
		get.SpaceVersion = request.SpaceVersion
		return c.transport.Get(ctx, c.addr, get)
	}
	setter := func(ctx context.Context, set cachewire.SetRequest) (cachewire.Record, error) {
		set.Key = request.Key
		set.NamespaceVersion = request.NamespaceVersion
		set.SpaceVersion = request.SpaceVersion
		return c.transport.Set(ctx, c.addr, set)
	}
	return applyCounter(ctx, request, getter, setter)
}

func (c *RoutedTCPClient) Counter(ctx context.Context, request CounterRequest) (CounterResult, error) {
	getter := func(ctx context.Context, get cachewire.GetRequest) (cachewire.Record, error) {
		get.Key = request.Key
		response, err := c.Get(ctx, get)
		return response, err
	}
	setter := func(ctx context.Context, set cachewire.SetRequest) (cachewire.Record, error) {
		set.Key = request.Key
		response, err := c.Set(ctx, set)
		return response, err
	}
	return applyCounter(ctx, request, getter, setter)
}

type counterGetFunc func(context.Context, cachewire.GetRequest) (cachewire.Record, error)
type counterSetFunc func(context.Context, cachewire.SetRequest) (cachewire.Record, error)

func applyCounter(ctx context.Context, request CounterRequest, get counterGetFunc, set counterSetFunc) (CounterResult, error) {
	maxRetries := request.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCounterMaxRetries
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		current, err := currentCounter(ctx, get, request)
		if err != nil {
			return CounterResult{}, err
		}

		setReq := cachewire.SetRequest{
			Key:              request.Key,
			Value:            []byte(strconv.FormatInt(current.next, 10)),
			ExpectedVersion:  current.version,
			TTLMillis:        current.ttlMillis,
			NamespaceVersion: current.namespaceVersion,
			SpaceVersion:     current.spaceVersion,
		}

		response, err := set(ctx, setReq)
		if err != nil {
			return CounterResult{}, err
		}
		if response.Found {
			return CounterResult{
				Record: response,
				Value:  current.next,
			}, nil
		}

		if attempt >= maxRetries {
			return CounterResult{}, fmt.Errorf("counter retries exhausted: %d", maxRetries)
		}
	}

	return CounterResult{}, fmt.Errorf("counter retries exhausted: %d", maxRetries)
}

type currentCounterResult struct {
	next             int64
	version          uint64
	ttlMillis        int64
	namespaceVersion uint64
	spaceVersion     uint64
}

func currentCounter(ctx context.Context, get counterGetFunc, request CounterRequest) (currentCounterResult, error) {
	record, err := get(ctx, cachewire.GetRequest{
		Key:              request.Key,
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
	})
	if err != nil {
		return currentCounterResult{}, err
	}

	if !record.Found {
		next, overflow := safeAdd(request.InitialValue, request.Delta)
		if overflow {
			return currentCounterResult{}, fmt.Errorf("counter arithmetic overflow")
		}
		return currentCounterResult{
			next:             next,
			version:          0,
			ttlMillis:        request.TTLMillis,
			namespaceVersion: request.NamespaceVersion,
			spaceVersion:     request.SpaceVersion,
		}, nil
	}

	decoded, err := parseInt64(record.Value)
	if err != nil {
		return currentCounterResult{}, err
	}

	next, overflow := safeAdd(decoded, request.Delta)
	if overflow {
		return currentCounterResult{}, fmt.Errorf("counter arithmetic overflow")
	}

	return currentCounterResult{
		next:             next,
		version:          record.Version,
		ttlMillis:        recordTTLMillis(record.ExpireAtUnixMs, time.Now()),
		namespaceVersion: record.NamespaceVersion,
		spaceVersion:     record.SpaceVersion,
	}, nil
}

func parseInt64(raw []byte) (int64, error) {
	text := strings.TrimSpace(string(raw))
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("counter value must be int64: %w", err)
	}
	return value, nil
}

func recordTTLMillis(expireAtUnixMs int64, now time.Time) int64 {
	if expireAtUnixMs <= 0 {
		return 0
	}
	remaining := expireAtUnixMs - now.UnixMilli()
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func safeAdd(base, delta int64) (int64, bool) {
	if delta > 0 && base > math.MaxInt64-delta {
		return 0, true
	}
	if delta < 0 && base < math.MinInt64-delta {
		return 0, true
	}
	return base + delta, false
}
