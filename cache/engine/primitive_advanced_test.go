package engine_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEnginePrimitiveBitmap(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "bitmap"}
	set, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:         engine.PrimitiveBitmapSetBit,
		Key:          key,
		Delta:        9,
		InitialValue: 1,
	})
	if err != nil {
		t.Fatalf("set bit: %v", err)
	}
	if set.Bool {
		t.Fatal("set bit previous = true, want false")
	}

	get, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveBitmapGetBit,
		Key:   key,
		Delta: 9,
	})
	if err != nil {
		t.Fatalf("get bit: %v", err)
	}
	if !get.Bool {
		t.Fatal("get bit = false, want true")
	}

	count, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: engine.PrimitiveBitmapBitCount,
		Key:  key,
	})
	if err != nil {
		t.Fatalf("bit count: %v", err)
	}
	if count.Count != 1 {
		t.Fatalf("bit count = %d, want 1", count.Count)
	}
}

func TestMemoryEnginePrimitiveHLL(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "hll"}
	for _, member := range []string{"alice", "bob", "alice"} {
		if _, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
			Kind:   engine.PrimitiveHLLAdd,
			Key:    key,
			Member: member,
		}); err != nil {
			t.Fatalf("hll add %s: %v", member, err)
		}
	}

	count, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: engine.PrimitiveHLLCount,
		Key:  key,
	})
	if err != nil {
		t.Fatalf("hll count: %v", err)
	}
	if count.Count != 2 {
		t.Fatalf("hll count = %d, want 2", count.Count)
	}
}

func TestMemoryEnginePrimitiveGeo(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "geo"}
	addGeoPoint(t, eng, key, "beijing", 116.4074, 39.9042)
	addGeoPoint(t, eng, key, "tianjin", 117.3616, 39.3434)
	addGeoPoint(t, eng, key, "shanghai", 121.4737, 31.2304)

	radius, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:     engine.PrimitiveGeoRadius,
		Key:      key,
		Score:    116.4074,
		MinScore: 39.9042,
		MaxScore: 150_000,
	})
	if err != nil {
		t.Fatalf("geo radius: %v", err)
	}
	if len(radius.ScoredMembers) != 2 {
		t.Fatalf("geo radius members = %v, want beijing and tianjin", radius.ScoredMembers)
	}
	if radius.ScoredMembers[0].Member != "beijing" || radius.ScoredMembers[1].Member != "tianjin" {
		t.Fatalf("geo radius members = %v", radius.ScoredMembers)
	}
}

func addGeoPoint(
	t *testing.T,
	eng engine.Engine,
	key engine.Key,
	member string,
	longitude float64,
	latitude float64,
) {
	t.Helper()
	if _, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:     engine.PrimitiveGeoAdd,
		Key:      key,
		Member:   member,
		Score:    longitude,
		MinScore: latitude,
	}); err != nil {
		t.Fatalf("geo add %s: %v", member, err)
	}
}
