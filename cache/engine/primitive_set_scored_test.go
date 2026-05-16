package engine_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEnginePrimitiveSet(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "tags"}
	addSetMember(t, eng, key, "blue")
	addSetMember(t, eng, key, "red")

	contains, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:   engine.PrimitiveSetContains,
		Key:    key,
		Member: "red",
	})
	if err != nil {
		t.Fatalf("set contains: %v", err)
	}
	if !contains.Found || !contains.Bool {
		t.Fatalf("set contains result = %+v", contains)
	}

	members, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: engine.PrimitiveSetMembers,
		Key:  key,
	})
	if err != nil {
		t.Fatalf("set members: %v", err)
	}
	assertStrings(t, members.Members, []string{"blue", "red"})

	removed, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:   engine.PrimitiveSetRemove,
		Key:    key,
		Member: "blue",
	})
	if err != nil {
		t.Fatalf("set remove: %v", err)
	}
	if !removed.Bool || removed.Count != 1 {
		t.Fatalf("set remove result = %+v", removed)
	}
}

func TestMemoryEnginePrimitiveScoredSet(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "rank"}
	putScore(t, eng, key, "alice", 2)
	putScore(t, eng, key, "bob", 1)
	putScore(t, eng, key, "cara", 3)

	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:        engine.PrimitiveScoredSetRange,
		Key:         key,
		MinScore:    1.5,
		MaxScore:    3,
		HasMinScore: true,
		HasMaxScore: true,
		Limit:       2,
	})
	if err != nil {
		t.Fatalf("scored range: %v", err)
	}
	assertScoredMembers(t, result.ScoredMembers, []engine.ScoredMember{
		{Member: "alice", Score: 2},
		{Member: "cara", Score: 3},
	})

	reverse, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:    engine.PrimitiveScoredSetRange,
		Key:     key,
		Reverse: true,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("reverse scored range: %v", err)
	}
	assertScoredMembers(t, reverse.ScoredMembers, []engine.ScoredMember{{Member: "cara", Score: 3}})
}

func addSetMember(t *testing.T, eng engine.Engine, key engine.Key, member string) {
	t.Helper()
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:   engine.PrimitiveSetAdd,
		Key:    key,
		Member: member,
	})
	if err != nil {
		t.Fatalf("set add %s: %v", member, err)
	}
	if !result.Applied {
		t.Fatalf("set add %s was not applied", member)
	}
}

func putScore(t *testing.T, eng engine.Engine, key engine.Key, member string, score float64) {
	t.Helper()
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:   engine.PrimitiveScoredSetPut,
		Key:    key,
		Member: member,
		Score:  score,
	})
	if err != nil {
		t.Fatalf("score put %s: %v", member, err)
	}
	if !result.Applied {
		t.Fatalf("score put %s was not applied", member)
	}
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("strings len = %d, want %d: %v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("strings[%d] = %q, want %q", index, got[index], want[index])
		}
	}
}

func assertScoredMembers(t *testing.T, got, want []engine.ScoredMember) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("scored len = %d, want %d: %+v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("scored[%d] = %+v, want %+v", index, got[index], want[index])
		}
	}
}
