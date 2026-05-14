package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
)

func main() {
	controlAddr := flag.String("control-addr", "127.0.0.1:7401", "control-plane HTTP address")
	namespace := flag.String("namespace", "orders", "cache namespace")
	space := flag.String("space", "session", "cache space")
	entity := flag.String("entity", "", "cache entity (optional)")
	key := flag.String("key", "smoke:1", "cache key")
	value := flag.String("value", "nespa-smoke", "cache value")
	flag.Parse()

	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: *controlAddr})
	if err != nil {
		log.Fatalf("new routed tcp: %v", err)
	}

	cacheKey := cachewire.Key{
		Namespace: strings.TrimSpace(*namespace),
		Space:     strings.TrimSpace(*space),
		Entity:    strings.TrimSpace(*entity),
		Key:       strings.TrimSpace(*key),
	}

	ctx := context.Background()

	if _, err := routed.Set(ctx, cachewire.SetRequest{
		Key:   cacheKey,
		Value: []byte(*value),
	}); err != nil {
		log.Fatalf("set: %v", err)
	}

	record, err := routed.Get(ctx, cachewire.GetRequest{Key: cacheKey})
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	if !record.Found {
		log.Fatal("record not found after set")
	}
	if string(record.Value) != *value {
		log.Fatalf("value mismatch: got=%q want=%q", string(record.Value), *value)
	}

	fmt.Println("routed tcp set/get ok")
	fmt.Println("smoke ok")
}
