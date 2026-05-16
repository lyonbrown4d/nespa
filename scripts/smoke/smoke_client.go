// Package main implements a lightweight smoke utility for routed TCP client checks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/client"
	"github.com/lyonbrown4d/nespa/controlapi"
	"github.com/lyonbrown4d/nespa/routing"
)

func main() {
	mode := flag.String("mode", "single", "smoke mode: single, multinode, or shrink")
	controlAddr := flag.String("control-addr", "127.0.0.1:7401", "control-plane HTTP address")
	namespace := flag.String("namespace", "orders", "cache namespace")
	space := flag.String("space", "session", "cache space")
	entity := flag.String("entity", "", "cache entity (optional)")
	key := flag.String("key", "smoke:1", "cache key")
	value := flag.String("value", "nespa-smoke", "cache value")
	quotaSmoke := flag.Bool("quota-smoke", false, "verify quota rejection with a large write")
	flag.Parse()

	cacheKey := cachewire.Key{
		Namespace: strings.TrimSpace(*namespace),
		Space:     strings.TrimSpace(*space),
		Entity:    strings.TrimSpace(*entity),
		Key:       strings.TrimSpace(*key),
	}

	ctx := context.Background()
	switch *mode {
	case "single":
		runSingle(ctx, *controlAddr, cacheKey, *value, *quotaSmoke)
	case "multinode":
		runMultinode(ctx, *controlAddr, cacheKey, *value)
	case "shrink":
		runShrink(ctx, *controlAddr, cacheKey, *value)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runSingle(ctx context.Context, controlAddr string, cacheKey cachewire.Key, value string, quotaSmoke bool) {
	routed := newRouted(controlAddr)
	if _, setErr := routed.Set(ctx, cachewire.SetRequest{
		Key:   cacheKey,
		Value: []byte(value),
	}); setErr != nil {
		log.Fatalf("set: %v", setErr)
	}

	record, err := routed.Get(ctx, cachewire.GetRequest{Key: cacheKey})
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	if !record.Found {
		log.Fatal("record not found after set")
	}
	if string(record.Value) != value {
		log.Fatalf("value mismatch: got=%q want=%q", string(record.Value), value)
	}
	runPrimitiveSmoke(ctx, routed, cacheKey, value)
	if quotaSmoke {
		runQuotaSmoke(ctx, routed, cacheKey)
		reseedAfterQuotaSmoke(ctx, routed, cacheKey, value)
	}

	if _, err := fmt.Fprintln(os.Stdout, "routed tcp set/get ok"); err != nil {
		log.Fatalf("write smoke output: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "smoke ok"); err != nil {
		log.Fatalf("write smoke output: %v", err)
	}
}

func reseedAfterQuotaSmoke(ctx context.Context, routed *client.RoutedTCPClient, baseKey cachewire.Key, value string) {
	reseedKey := baseKey
	reseedKey.Key += ":post-quota"
	if _, err := routed.Set(ctx, cachewire.SetRequest{
		Key:   reseedKey,
		Value: []byte(value + "-post-quota"),
	}); err != nil {
		log.Fatalf("post-quota set: %v", err)
	}
	record, err := routed.Get(ctx, cachewire.GetRequest{Key: reseedKey})
	if err != nil {
		log.Fatalf("post-quota get: %v", err)
	}
	if !record.Found || string(record.Value) != value+"-post-quota" {
		log.Fatalf("post-quota get = %+v, want %q", record, value+"-post-quota")
	}
}

func runMultinode(ctx context.Context, controlAddr string, baseKey cachewire.Key, value string) {
	snapshot := fetchSnapshot(ctx, controlAddr)
	routes := scopedRoutes(snapshot, baseKey.Namespace, baseKey.Space)
	if len(routes) < 2 {
		log.Fatalf("expected at least two scoped routes, got %d", len(routes))
	}

	first := routes[0]
	second := routes[1]
	firstKey := baseKey
	firstKey.Key = keyForRoute(baseKey.Namespace, baseKey.Space, first, "multi-a")
	secondKey := baseKey
	secondKey.Key = keyForRoute(baseKey.Namespace, baseKey.Space, second, "multi-b")

	routed := newRouted(controlAddr)
	set, err := routed.BatchSet(ctx, cachewire.BatchSetRequest{Items: []cachewire.SetRequest{
		{Key: firstKey, Value: []byte(value + "-a")},
		{Key: secondKey, Value: []byte(value + "-b")},
	}})
	if err != nil {
		log.Fatalf("batch set: %v", err)
	}
	if len(set.Records) != 2 || !set.Records[0].Found || !set.Records[1].Found {
		log.Fatalf("unexpected batch set response: %+v", set)
	}

	get, err := routed.BatchGet(ctx, cachewire.BatchGetRequest{Items: []cachewire.GetRequest{
		{Key: firstKey},
		{Key: secondKey},
	}})
	if err != nil {
		log.Fatalf("batch get: %v", err)
	}
	requireBatchValue(get, 0, value+"-a")
	requireBatchValue(get, 1, value+"-b")
	requireDirectValue(ctx, first.Addr, firstKey, value+"-a")
	requireDirectValue(ctx, second.Addr, secondKey, value+"-b")
	runMultinodePrimitiveSmoke(ctx, routed, first, second, firstKey, secondKey, value)

	if _, err := fmt.Fprintln(os.Stdout, "routed tcp multinode batch ok"); err != nil {
		log.Fatalf("write smoke output: %v", err)
	}
}

func runShrink(ctx context.Context, controlAddr string, cacheKey cachewire.Key, value string) {
	snapshot := fetchSnapshot(ctx, controlAddr)
	routes := scopedRoutes(snapshot, cacheKey.Namespace, cacheKey.Space)
	if len(routes) != 1 {
		log.Fatalf("expected one scoped route after shrink, got %d", len(routes))
	}

	routed := newRouted(controlAddr)
	if _, err := routed.Set(ctx, cachewire.SetRequest{Key: cacheKey, Value: []byte(value)}); err != nil {
		log.Fatalf("set after route shrink: %v", err)
	}
	requireDirectValue(ctx, routes[0].Addr, cacheKey, value)

	if _, err := fmt.Fprintln(os.Stdout, "routed tcp route shrink ok"); err != nil {
		log.Fatalf("write smoke output: %v", err)
	}
}

func newRouted(controlAddr string) *client.RoutedTCPClient {
	routed, err := client.NewRoutedTCP(client.RoutedConfig{ControlAddr: controlAddr})
	if err != nil {
		log.Fatalf("new routed tcp: %v", err)
	}
	return routed
}

func fetchSnapshot(ctx context.Context, controlAddr string) controlapi.SnapshotBody {
	snapshot, err := loadSnapshot(ctx, controlAddr)
	if err != nil {
		log.Fatalf("fetch snapshot: %v", err)
	}
	return snapshot
}

func loadSnapshot(ctx context.Context, controlAddr string) (controlapi.SnapshotBody, error) {
	base := strings.TrimRight(controlAddr, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/control/snapshot", http.NoBody)
	if err != nil {
		return controlapi.SnapshotBody{}, fmt.Errorf("create snapshot request: %w", err)
	}
	httpClient := http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return controlapi.SnapshotBody{}, fmt.Errorf("request snapshot: %w", err)
	}
	defer closeSnapshotBody(resp.Body)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return controlapi.SnapshotBody{}, fmt.Errorf("snapshot status: %s", resp.Status)
	}

	var snapshot controlapi.SnapshotBody
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return controlapi.SnapshotBody{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}

func closeSnapshotBody(body io.Closer) {
	if err := body.Close(); err != nil {
		log.Printf("close snapshot body: %v", err)
	}
}

func scopedRoutes(snapshot controlapi.SnapshotBody, namespace, space string) []controlapi.RouteBody {
	var routes []controlapi.RouteBody
	for _, route := range snapshot.Routes {
		if route.Namespace == namespace && route.Space == space {
			routes = append(routes, route)
		}
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].VSlotStart < routes[j].VSlotStart
	})
	return routes
}

func keyForRoute(namespace, space string, route controlapi.RouteBody, prefix string) string {
	for seq := range 1_000_000 {
		key := fmt.Sprintf("%s:%d", prefix, seq)
		slot := routing.VSlotFor(namespace, space, key)
		if routing.ContainsVSlot(route, slot) {
			return key
		}
	}
	log.Fatalf("could not find key for route %+v", route)
	return ""
}

func requireBatchValue(response cachewire.BatchGetResponse, index int, want string) {
	if index >= len(response.Records) {
		log.Fatalf("batch get record %d missing: %+v", index, response)
	}
	record := response.Records[index]
	if !record.Found || string(record.Value) != want {
		log.Fatalf("batch get record %d = %+v, want %q", index, record, want)
	}
}

func requireDirectValue(ctx context.Context, addr string, key cachewire.Key, want string) {
	direct, err := client.NewTCP(client.Config{Addr: addr})
	if err != nil {
		log.Fatalf("new direct tcp client: %v", err)
	}
	record, err := direct.Get(ctx, cachewire.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("direct get %s: %v", addr, err)
	}
	if !record.Found || string(record.Value) != want {
		log.Fatalf("direct get %s = %+v, want %q", addr, record, want)
	}
}
