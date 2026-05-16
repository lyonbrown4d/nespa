package engine_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineDistinguishesEntitiesInPhysicalKeys(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 1})
	defer closeEngine(t, eng)

	keyA := engine.Key{Namespace: "ns", Space: "sp", Entity: "OrderView", Key: "k"}
	keyB := engine.Key{Namespace: "ns", Space: "sp", Entity: "SessionView", Key: "k"}

	setEngineValue(t, eng, keyA, "order")
	setEngineValue(t, eng, keyB, "session")

	requireEngineValue(t, eng, keyA, "order", "entity A")
	requireEngineValue(t, eng, keyB, "session", "entity B")

	missingKey := keyA
	missingKey.Entity = "InvoiceView"
	requireEngineMissing(t, eng, missingKey, "missing entity")
}

func setEngineValue(t *testing.T, eng engine.Engine, key engine.Key, value string) {
	t.Helper()
	if _, _, err := eng.Set(context.Background(), key, []byte(value), engine.SetOptions{}); err != nil {
		t.Fatalf("set %s: %v", key.Key, err)
	}
}

func requireEngineValue(t *testing.T, eng engine.Engine, key engine.Key, want, name string) {
	t.Helper()
	record, ok, err := eng.Get(context.Background(), key, engine.GetOptions{})
	if err != nil || !ok {
		t.Fatalf("get %s: ok=%v err=%v", name, ok, err)
	}
	if string(record.Value) != want {
		t.Fatalf("%s value = %q, want %q", name, string(record.Value), want)
	}
}
