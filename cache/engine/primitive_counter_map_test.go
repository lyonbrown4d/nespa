package engine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEnginePrimitiveCounterAdjust(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "counter"}
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:         engine.PrimitiveCounterAdjust,
		Key:          key,
		Delta:        2,
		InitialValue: 40,
	})
	if err != nil {
		t.Fatalf("adjust counter: %v", err)
	}
	if !result.Applied || string(result.Value) != "42" {
		t.Fatalf("counter result = %+v", result)
	}

	stale, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: engine.PrimitiveCounterAdjust,
		Key:  key,
		Options: engine.PrimitiveOptions{
			ExpectedVersion: result.Record.Version + 1,
		},
		Delta: 1,
	})
	if err != nil {
		t.Fatalf("stale adjust: %v", err)
	}
	if stale.Applied || stale.Found {
		t.Fatalf("stale adjust result = %+v, want miss", stale)
	}
}

func TestMemoryEnginePrimitiveMap(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "profile"}
	setMapField(t, eng, key, "name", "alice")
	setMapField(t, eng, key, "role", "admin")

	got, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveMapGet,
		Key:   key,
		Field: "name",
	})
	if err != nil {
		t.Fatalf("map get: %v", err)
	}
	if !got.Found || string(got.Value) != "alice" {
		t.Fatalf("map get result = %+v", got)
	}

	all, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: engine.PrimitiveMapGetAll,
		Key:  key,
	})
	if err != nil {
		t.Fatalf("map get all: %v", err)
	}
	assertMapFields(t, all.Fields, []engine.MapField{
		{Field: "name", Value: []byte("alice")},
		{Field: "role", Value: []byte("admin")},
	})

	deleted, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveMapDelete,
		Key:   key,
		Field: "role",
	})
	if err != nil {
		t.Fatalf("map delete: %v", err)
	}
	if !deleted.Bool || deleted.Count != 1 {
		t.Fatalf("map delete result = %+v", deleted)
	}
}

func setMapField(t *testing.T, eng engine.Engine, key engine.Key, field, value string) {
	t.Helper()
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveMapSet,
		Key:   key,
		Field: field,
		Value: []byte(value),
	})
	if err != nil {
		t.Fatalf("map set %s: %v", field, err)
	}
	if !result.Applied {
		t.Fatalf("map set %s was not applied", field)
	}
}

func assertMapFields(t *testing.T, got, want []engine.MapField) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("field len = %d, want %d: %+v", len(got), len(want), got)
	}
	for index := range want {
		if got[index].Field != want[index].Field || !bytes.Equal(got[index].Value, want[index].Value) {
			t.Fatalf("field[%d] = %+v, want %+v", index, got[index], want[index])
		}
	}
}
