// Package main demonstrates direct TCP access through the Nespa Go SDK.
package main

import (
	"context"
	"log"
	"time"

	nespa "github.com/lyonbrown4d/nespa/sdk/go"
)

func main() {
	ctx := context.Background()
	client, err := nespa.NewDirect("127.0.0.1:7403")
	if err != nil {
		log.Fatal(err)
	}

	key := nespa.Key{Namespace: "orders", Space: "session", Entity: "SessionView", Key: "demo"}
	record, err := client.Set(ctx, key, []byte("value"), nespa.SetOptions{TTL: time.Minute})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored version=%d", record.Version)
}
