package main

import (
	"reflect"
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
		"--frontend-addr", "127.0.0.1:9002",
		"--frontend-node-addr", "127.0.0.1:9103",
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

func TestFrontendConfigFallsBackToNodeAddress(t *testing.T) {
	cfg := serverConfig{
		Control: controlConfig{Addr: "127.0.0.1:7401"},
		Frontend: frontendConfig{
			Addr: "127.0.0.1:7402",
		},
		Node: nodeConfig{Addr: "127.0.0.1:7403"},
	}

	frontendCfg := frontendConfigFrom(cfg)
	if frontendCfg.NodeAddr != "127.0.0.1:7403" {
		t.Fatalf("frontend node addr = %s, want node addr fallback", frontendCfg.NodeAddr)
	}
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
		{name: "frontend addr", got: cfg.Frontend.Addr, want: "127.0.0.1:9002"},
		{name: "frontend node addr", got: cfg.Frontend.Node.Addr, want: "127.0.0.1:9103"},
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
