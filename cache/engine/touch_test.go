package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestMemoryEngineTouchRejectsMismatchedVersion(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "view", Key: "k1"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{
		TTL:              time.Second,
		NamespaceVersion: 1,
		SpaceVersion:     1,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	now = now.Add(500 * time.Millisecond)
	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{
		TTL:              2 * time.Second,
		NamespaceVersion: 2,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("touch mismatched version: %v", err)
	}
	if touched {
		t.Fatal("touch with mismatched namespace version succeeded")
	}

	now = now.Add(600 * time.Millisecond)
	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{NamespaceVersion: 1, SpaceVersion: 1})
	if err != nil || ok {
		t.Fatalf("record after mismatched touch: ok=%v err=%v", ok, err)
	}
}

func TestMemoryEngineTouchExtendsMatchingVersion(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "view", Key: "k1"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{
		TTL:              time.Second,
		NamespaceVersion: 1,
		SpaceVersion:     1,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	now = now.Add(500 * time.Millisecond)
	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{
		TTL:              2 * time.Second,
		NamespaceVersion: 1,
		SpaceVersion:     1,
	})
	if err != nil {
		t.Fatalf("touch matching version: %v", err)
	}
	if !touched {
		t.Fatal("touch with matching version missed")
	}

	now = now.Add(700 * time.Millisecond)
	if _, ok, err := eng.Get(context.Background(), key, engine.GetOptions{NamespaceVersion: 1, SpaceVersion: 1}); err != nil || !ok {
		t.Fatalf("record after matching touch: ok=%v err=%v", ok, err)
	}
}

func TestMemoryEngineTouchRejectsNegativeTTL(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "view", Key: "k2"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set: %v", err)
	}

	now = now.Add(500 * time.Millisecond)
	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{
		TTL:              -1 * time.Second,
		NamespaceVersion: 0,
		SpaceVersion:     0,
	})
	if err != nil {
		t.Fatalf("touch negative ttl: %v", err)
	}
	if touched {
		t.Fatal("negative ttl touch should not succeed")
	}
}

func TestMemoryEngineTouchZeroTTLRemovesExpiration(t *testing.T) {
	now := time.UnixMilli(1000)
	eng := engine.NewMemory(engine.Config{ShardCount: 2, Now: func() time.Time { return now }})
	defer closeEngine(t, eng)

	key := engine.Key{Namespace: "order", Space: "view", Key: "k3"}
	if _, err := eng.Set(context.Background(), key, []byte("value"), engine.SetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("set: %v", err)
	}

	now = now.Add(500 * time.Millisecond)
	touched, err := eng.Touch(context.Background(), key, engine.TouchOptions{
		TTL:              0,
		NamespaceVersion: 0,
		SpaceVersion:     0,
	})
	if err != nil {
		t.Fatalf("touch zero ttl: %v", err)
	}
	if !touched {
		t.Fatal("zero ttl touch should succeed")
	}

	now = now.Add(2 * time.Second)
	_, ok, err := eng.Get(context.Background(), key, engine.GetOptions{NamespaceVersion: 0, SpaceVersion: 0})
	if err != nil || !ok {
		t.Fatalf("record after zero-ttl touch: ok=%v err=%v", ok, err)
	}
}
