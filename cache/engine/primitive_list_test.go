package engine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEnginePrimitiveList(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 2})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "n", Space: "s", Key: "queue"}
	pushListValue(t, eng, key, engine.PrimitiveListPushBack, "middle")
	pushListValue(t, eng, key, engine.PrimitiveListPushFront, "first")
	pushListValue(t, eng, key, engine.PrimitiveListPushBack, "last")

	rangeResult, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  engine.PrimitiveListRange,
		Key:   key,
		Start: 1,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("list range: %v", err)
	}
	assertListValues(t, rangeResult.Values, []string{"middle", "last"})
	if rangeResult.Count != 3 {
		t.Fatalf("list range count = %d, want 3", rangeResult.Count)
	}

	reverse, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:    engine.PrimitiveListRange,
		Key:     key,
		Reverse: true,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("reverse list range: %v", err)
	}
	assertListValues(t, reverse.Values, []string{"last", "middle"})

	front := popListValue(t, eng, key, engine.PrimitiveListPopFront)
	if string(front.Value) != "first" || front.Count != 2 {
		t.Fatalf("front pop = %+v, want first count 2", front)
	}
	back := popListValue(t, eng, key, engine.PrimitiveListPopBack)
	if string(back.Value) != "last" || back.Count != 1 {
		t.Fatalf("back pop = %+v, want last count 1", back)
	}
}

func pushListValue(
	t *testing.T,
	eng engine.Engine,
	key engine.Key,
	kind engine.PrimitiveKind,
	value string,
) {
	t.Helper()
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind:  kind,
		Key:   key,
		Value: []byte(value),
	})
	if err != nil {
		t.Fatalf("list push %s: %v", value, err)
	}
	if !result.Applied {
		t.Fatalf("list push %s was not applied", value)
	}
}

func popListValue(t *testing.T, eng engine.Engine, key engine.Key, kind engine.PrimitiveKind) engine.PrimitiveResult {
	t.Helper()
	result, err := eng.Primitive(context.Background(), engine.PrimitiveRequest{
		Kind: kind,
		Key:  key,
	})
	if err != nil {
		t.Fatalf("list pop: %v", err)
	}
	if !result.Found || !result.Applied {
		t.Fatalf("list pop result = %+v, want found applied", result)
	}
	return result
}

func assertListValues(t *testing.T, got [][]byte, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("list values len = %d, want %d: %+v", len(got), len(want), got)
	}
	for index := range want {
		if !bytes.Equal(got[index], []byte(want[index])) {
			t.Fatalf("list value[%d] = %q, want %q", index, got[index], want[index])
		}
	}
}
