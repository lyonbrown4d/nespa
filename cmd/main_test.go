package main

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestLoadServerConfigFromFlags(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	addServerFlags(flags)
	if err := flags.Parse([]string{
		"--control-addr", "127.0.0.1:9001",
		"--control-cluster-id", "smoke",
		"--control-liveness-sweep-interval", "1s",
		"--control-liveness-suspect-after", "2s",
		"--control-liveness-dead-after", "3s",
		"--frontend-enabled=false",
		"--frontend-addr", "127.0.0.1:9002",
		"--admin-enabled=false",
		"--node-addr", "127.0.0.1:9003",
		"--node-id", "node-a",
		"--node-heartbeat-interval", "4s",
		"--admin-addr", "127.0.0.1:9004",
		"--node-quota-namespace-memory-bytes", "1024",
		"--node-quota-space-memory-bytes", "2048",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg, err := loadServerConfig(configSource{
		Flags:     flags,
		EnvPrefix: "NESPA",
		Defaults:  serverDefaults(),
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	assertConfigValues(t, cfg)
}

func TestFrontendConfigUsesControlSnapshotOnly(t *testing.T) {
	cfg := serverConfig{
		Control: controlConfig{Addr: "127.0.0.1:7401"},
		Frontend: frontendConfig{
			Addr: "127.0.0.1:7402",
		},
		Node: nodeConfig{Addr: "127.0.0.1:7403"},
	}

	frontendCfg := frontendConfigFrom(cfg)
	if frontendCfg.Addr != "127.0.0.1:7402" || frontendCfg.ControlAddr != "127.0.0.1:7401" {
		t.Fatalf("frontend config = %+v", frontendCfg)
	}
}

func TestEndpoints(t *testing.T) {
	cfg := serverConfig{
		Control: controlConfig{Addr: "127.0.0.1:7401"},
		Node:    nodeConfig{Addr: "127.0.0.1:7403"},
		Frontend: frontendConfig{
			Enabled: true,
			Addr:    "127.0.0.1:7402",
		},
		Admin: endpointConfig{
			Enabled: true,
			Addr:    "127.0.0.1:7404",
		},
	}
	got := endpointsForTest(t, endpoints(cfg))
	expect := []string{"control", "node", "frontend", "admin"}
	if !slices.Equal(got, expect) {
		t.Fatalf("endpoints() = %v, want %v", got, expect)
	}

	cfg.Frontend.Enabled = false
	cfg.Admin.Enabled = false
	got = endpointsForTest(t, endpoints(cfg))
	expect = []string{"control", "node"}
	if !slices.Equal(got, expect) {
		t.Fatalf("endpoints() = %v, want %v", got, expect)
	}
}

func endpointsForTest(t *testing.T, values *collectionlist.List[endpointInfo]) []string {
	t.Helper()

	if values == nil {
		return nil
	}

	names := make([]string, 0, values.Len())
	values.Range(func(_ int, endpoint endpointInfo) bool {
		names = append(names, endpoint.name)
		return true
	})
	return names
}

func assertConfigValues(t *testing.T, cfg serverConfig) {
	t.Helper()
	checks := []struct {
		name string
		got  any
		want any
	}{
		{name: "control addr", got: cfg.Control.Addr, want: "127.0.0.1:9001"},
		{name: "cluster id", got: cfg.Control.Cluster.ID, want: "smoke"},
		{name: "liveness sweep", got: cfg.Control.Liveness.Sweep.Interval, want: time.Second},
		{name: "liveness suspect", got: cfg.Control.Liveness.Suspect.After, want: 2 * time.Second},
		{name: "liveness dead", got: cfg.Control.Liveness.Dead.After, want: 3 * time.Second},
		{name: "frontend enabled", got: cfg.Frontend.Enabled, want: false},
		{name: "frontend addr", got: cfg.Frontend.Addr, want: "127.0.0.1:9002"},
		{name: "admin enabled", got: cfg.Admin.Enabled, want: false},
		{name: "node addr", got: cfg.Node.Addr, want: "127.0.0.1:9003"},
		{name: "node id", got: cfg.Node.ID, want: "node-a"},
		{name: "heartbeat interval", got: cfg.Node.Heartbeat.Interval, want: 4 * time.Second},
		{name: "admin addr", got: cfg.Admin.Addr, want: "127.0.0.1:9004"},
		{name: "namespace quota", got: cfg.Node.Quota.Namespace.Memory.Bytes, want: uint64(1024)},
		{name: "space quota", got: cfg.Node.Quota.Space.Memory.Bytes, want: uint64(2048)},
	}
	for _, check := range checks {
		if !reflect.DeepEqual(check.got, check.want) {
			t.Fatalf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}
