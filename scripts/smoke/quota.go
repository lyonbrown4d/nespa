package main

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/protocol"
)

func runQuotaSmoke(ctx context.Context, routed *client.RoutedTCPClient, baseKey cachewire.Key) {
	quotaKey := baseKey
	quotaKey.Key += ":quota"
	_, err := routed.Set(ctx, cachewire.SetRequest{
		Key:   quotaKey,
		Value: []byte(strings.Repeat("x", 2*1024*1024)),
	})
	if err == nil {
		log.Fatal("quota smoke write succeeded, want quota rejection")
	}
	var wireErr cachewire.Error
	if !errors.As(err, &wireErr) || wireErr.Code != protocol.ErrorTooLarge {
		log.Fatalf("quota smoke error = %v, want ErrorTooLarge", err)
	}
}
