package engine_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineEstimatePrimitiveDoesNotMutate(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "profile"}
	request := engine.PrimitiveRequest{
		Kind:  engine.PrimitiveMapSet,
		Key:   key,
		Field: "name",
		Value: []byte("alice"),
	}
	estimate, err := eng.EstimatePrimitive(context.Background(), request)
	if err != nil {
		t.Fatalf("estimate primitive: %v", err)
	}
	if !estimate.Applied || estimate.OldCostBytes != 0 || estimate.NewCostBytes == 0 {
		t.Fatalf("estimate = %+v", estimate)
	}
	_, found, getErr := eng.Get(context.Background(), key, engine.GetOptions{})
	if getErr != nil || found {
		t.Fatalf("estimate mutated record: found=%v err=%v", found, getErr)
	}

	result, err := eng.Primitive(context.Background(), request)
	if err != nil {
		t.Fatalf("primitive: %v", err)
	}
	if result.Record.CostBytes != estimate.NewCostBytes {
		t.Fatalf("cost = %d, want estimate %d", result.Record.CostBytes, estimate.NewCostBytes)
	}
}

func TestMemoryEngineEstimatePrimitiveExpectedVersionMiss(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "profile"}
	estimate, err := eng.EstimatePrimitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveMapSet,
		Key:   key,
		Field: "name",
		Value: []byte("alice"),
		Options: engine.PrimitiveOptions{
			ExpectedVersion: 1,
		},
	})
	if err != nil {
		t.Fatalf("estimate primitive: %v", err)
	}
	if estimate.Applied {
		t.Fatalf("estimate applied on missing expected version: %+v", estimate)
	}
}
