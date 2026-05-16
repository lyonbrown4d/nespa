package client

import (
	"context"
	"errors"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

func TestApplyCounterRetriesWhenSetVersionMismatches(t *testing.T) {
	script := &retryCounterScript{}

	result, err := applyCounter(context.Background(), CounterRequest{
		Key:        cachewire.Key{Namespace: "ns", Space: "sp", Key: "k"},
		Delta:      1,
		MaxRetries: 3,
	}, script.get, script.set)
	if err != nil {
		t.Fatalf("applyCounter: %v", err)
	}
	script.assert(t, result)
}

type retryCounterScript struct {
	getCalls            int
	setCalls            int
	setExpectedVersions []uint64
}

func (s *retryCounterScript) get(context.Context, cachewire.GetRequest) (cachewire.Record, error) {
	s.getCalls++
	switch s.getCalls {
	case 1:
		return counterRecord(1), nil
	case 2:
		return counterRecord(2), nil
	default:
		return cachewire.Record{}, errors.New("unexpected get call")
	}
}

func (s *retryCounterScript) set(_ context.Context, request cachewire.SetRequest) (cachewire.Record, error) {
	s.setCalls++
	s.setExpectedVersions = append(s.setExpectedVersions, request.ExpectedVersion)
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

func (s *retryCounterScript) assert(t *testing.T, result CounterResult) {
	t.Helper()

	if result.Value != 11 {
		t.Fatalf("result value = %d, want 11", result.Value)
	}
	if s.getCalls != 2 {
		t.Fatalf("get calls = %d, want 2", s.getCalls)
	}
	if s.setCalls != 2 {
		t.Fatalf("set calls = %d, want 2", s.setCalls)
	}
	if len(s.setExpectedVersions) != 2 {
		t.Fatalf("set expected versions = %d, want 2", len(s.setExpectedVersions))
	}
	if s.setExpectedVersions[0] != 1 || s.setExpectedVersions[1] != 2 {
		t.Fatalf("set expected versions = %#v, want [1 2]", s.setExpectedVersions)
	}
	if result.Record.NamespaceVersion != 3 || result.Record.SpaceVersion != 4 {
		t.Fatalf("record versions = %d/%d, want 3/4", result.Record.NamespaceVersion, result.Record.SpaceVersion)
	}
}

func counterRecord(version uint64) cachewire.Record {
	return cachewire.Record{
		Found:            true,
		Value:            []byte("10"),
		Version:          version,
		NamespaceVersion: 3,
		SpaceVersion:     4,
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
