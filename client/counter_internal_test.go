package client

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestApplyCounterRetriesWhenSetVersionMismatches(t *testing.T) {
	getCalls := 0
	setCalls := 0
	var setExpectedVersions []uint64

	get := func(context.Context, cachewire.GetRequest) (cachewire.Record, error) {
		getCalls++
		switch getCalls {
		case 1:
			return cachewire.Record{
				Found:            true,
				Value:            []byte("10"),
				Version:          1,
				NamespaceVersion: 3,
				SpaceVersion:     4,
			}, nil
		case 2:
			return cachewire.Record{
				Found:            true,
				Value:            []byte("10"),
				Version:          2,
				NamespaceVersion: 3,
				SpaceVersion:     4,
			}, nil
		default:
			return cachewire.Record{}, errors.New("unexpected get call")
		}
	}

	set := func(_ context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
		setCalls++
		setExpectedVersions = append(setExpectedVersions, request.ExpectedVersion)

		if request.ExpectedVersion == 1 {
			return cachewire.Record{Found: false}, nil
		}

		return cachewire.Record{
			Found:            true,
			Version:          3,
			NamespaceVersion: request.NamespaceVersion,
			SpaceVersion:     request.SpaceVersion,
		}, nil
	}

	result, err := applyCounter(context.Background(), CounterRequest{
		Key:        cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"},
		Delta:      1,
		MaxRetries: 3,
	}, get, set)
	if err != nil {
		t.Fatalf("applyCounter: %v", err)
	}
	if result.Value != 11 {
		t.Fatalf("result value = %d, want 11", result.Value)
	}
	if getCalls != 2 {
		t.Fatalf("get calls = %d, want 2", getCalls)
	}
	if setCalls != 2 {
		t.Fatalf("set calls = %d, want 2", setCalls)
	}
	if len(setExpectedVersions) != 2 {
		t.Fatalf("set expected versions = %d, want 2", len(setExpectedVersions))
	}
	if setExpectedVersions[0] != 1 || setExpectedVersions[1] != 2 {
		t.Fatalf("set expected versions = %#v, want [1 2]", setExpectedVersions)
	}
	if result.Record.NamespaceVersion != 3 || result.Record.SpaceVersion != 4 {
		t.Fatalf("record versions = %d/%d, want 3/4", result.Record.NamespaceVersion, result.Record.SpaceVersion)
	}
}

func TestApplyCounterCreatesMissingWithRequestVersionsAndTTL(t *testing.T) {
	expectedTTL := int64(15000)
	set := func(_ context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
		if request.ExpectedVersion != 0 {
			t.Fatalf("expected set expected version 0, got %d", request.ExpectedVersion)
		}
		if request.TTLMillis != expectedTTL {
			t.Fatalf("ttl = %d, want %d", request.TTLMillis, expectedTTL)
		}
		if request.NamespaceVersion != 9 || request.SpaceVersion != 11 {
			t.Fatalf("record versions = %d/%d, want 9/11", request.NamespaceVersion, request.SpaceVersion)
		}
		return cachewire.Record{
			Found:            true,
			NamespaceVersion: 9,
			SpaceVersion:     11,
		}, nil
	}

	result, err := applyCounter(context.Background(), CounterRequest{
		Key:              cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"},
		InitialValue:     8,
		Delta:            2,
		TTLMillis:        expectedTTL,
		NamespaceVersion: 9,
		SpaceVersion:     11,
	}, func(context.Context, cachewire.GetRequest) (cachewire.Record, error) {
		return cachewire.Record{}, nil
	}, set)
	if err != nil {
		t.Fatalf("applyCounter: %v", err)
	}
	if result.Value != 10 {
		t.Fatalf("result value = %d, want 10", result.Value)
	}
	if !result.Record.Found {
		t.Fatalf("result record should be found")
	}
}
