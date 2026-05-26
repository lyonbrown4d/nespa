package cache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
)

func TestEngineServiceTransactionSerializesOperations(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	key := cache.Key{Namespace: "order", Space: "session", Key: "tx"}
	counter := cache.Key{Namespace: "order", Space: "session", Key: "counter"}

	err := svc.Transaction(context.Background(), func(ctx context.Context, tx cache.Service) error {
		if _, err := tx.Set(ctx, key, []byte("one"), cache.SetOptions{}); err != nil {
			return fmt.Errorf("transaction set: %w", err)
		}
		record, found, err := tx.Get(ctx, key, cache.GetOptions{})
		if err != nil {
			return fmt.Errorf("transaction get: %w", err)
		}
		if !found || string(record.Value) != "one" {
			t.Fatalf("transaction read = found %t value %q, want one", found, record.Value)
		}
		if _, err := tx.Adjust(ctx, counter, cache.AdjustOptions{Delta: 3}); err != nil {
			return fmt.Errorf("transaction adjust: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	requireServiceValue(t, svc, key, "one")
	requireServiceValue(t, svc, counter, "3")
}

func TestEngineServiceRejectsNestedTransaction(t *testing.T) {
	eng := engine.NewMemory(engine.Config{ShardCount: 4})
	defer closeEngine(t, eng)

	svc := cache.NewService(eng)
	err := svc.Transaction(context.Background(), func(ctx context.Context, tx cache.Service) error {
		if err := tx.Transaction(ctx, func(context.Context, cache.Service) error {
			return nil
		}); err != nil {
			return fmt.Errorf("nested transaction: %w", err)
		}
		return nil
	})
	if !errors.Is(err, cache.ErrNestedTransactionUnsupported) {
		t.Fatalf("nested transaction error = %v, want ErrNestedTransactionUnsupported", err)
	}
}
