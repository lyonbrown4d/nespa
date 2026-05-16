package main

import (
	"context"
	"log"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func runPrimitiveSmoke(ctx context.Context, routed *client.RoutedTCPClient, baseKey cachewire.Key, value string) {
	primitiveKey := baseKey
	primitiveKey.Key += ":primitive"
	if _, err := routed.Primitive(ctx, cachewire.PrimitiveRequest{
		Key:   primitiveKey,
		Kind:  cachewire.PrimitiveMapSet,
		Field: "name",
		Value: []byte(value),
	}); err != nil {
		log.Fatalf("primitive map set: %v", err)
	}
	result, err := routed.Primitive(ctx, cachewire.PrimitiveRequest{
		Key:   primitiveKey,
		Kind:  cachewire.PrimitiveMapGet,
		Field: "name",
	})
	if err != nil {
		log.Fatalf("primitive map get: %v", err)
	}
	requirePrimitiveValue(result, value, "primitive map get")

	batchKey := baseKey
	batchKey.Key += ":batch-primitive"
	batch, err := routed.BatchPrimitive(ctx, cachewire.BatchPrimitiveRequest{
		Items: []cachewire.PrimitiveRequest{
			{Key: batchKey, Kind: cachewire.PrimitiveMapSet, Field: "name", Value: []byte(value + "-batch")},
			{Key: batchKey, Kind: cachewire.PrimitiveMapGet, Field: "name"},
		},
	})
	if err != nil {
		log.Fatalf("batch primitive: %v", err)
	}
	if len(batch.Results) != 2 {
		log.Fatalf("batch primitive result count = %d, want 2", len(batch.Results))
	}
	requirePrimitiveValue(batch.Results[1], value+"-batch", "batch primitive map get")
}

func runMultinodePrimitiveSmoke(
	ctx context.Context,
	routed *client.RoutedTCPClient,
	first controlapi.RouteBody,
	second controlapi.RouteBody,
	firstKey cachewire.Key,
	secondKey cachewire.Key,
	value string,
) {
	firstPrimitive := firstKey
	firstPrimitive.Key += ":primitive"
	secondPrimitive := secondKey
	secondPrimitive.Key += ":primitive"
	response, err := routed.BatchPrimitive(ctx, cachewire.BatchPrimitiveRequest{
		Items: []cachewire.PrimitiveRequest{
			{Key: firstPrimitive, Kind: cachewire.PrimitiveMapSet, Field: "name", Value: []byte(value + "-pa")},
			{Key: secondPrimitive, Kind: cachewire.PrimitiveMapSet, Field: "name", Value: []byte(value + "-pb")},
		},
	})
	if err != nil {
		log.Fatalf("multinode batch primitive: %v", err)
	}
	if len(response.Results) != 2 || !response.Results[0].Applied || !response.Results[1].Applied {
		log.Fatalf("unexpected multinode batch primitive response: %+v", response)
	}
	requireDirectPrimitiveValue(ctx, first.Addr, firstPrimitive, value+"-pa")
	requireDirectPrimitiveValue(ctx, second.Addr, secondPrimitive, value+"-pb")
}

func requireDirectPrimitiveValue(ctx context.Context, addr string, key cachewire.Key, want string) {
	direct, err := client.NewTCP(client.Config{Addr: addr})
	if err != nil {
		log.Fatalf("new direct tcp client: %v", err)
	}
	result, err := direct.Primitive(ctx, cachewire.PrimitiveRequest{
		Key:   key,
		Kind:  cachewire.PrimitiveMapGet,
		Field: "name",
	})
	if err != nil {
		log.Fatalf("direct primitive get %s: %v", addr, err)
	}
	requirePrimitiveValue(result, want, "direct primitive get")
}

func requirePrimitiveValue(result cachewire.PrimitiveResult, want, name string) {
	if !result.Found || string(result.Value) != want {
		log.Fatalf("%s = %+v, want %q", name, result, want)
	}
}
