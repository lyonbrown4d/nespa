package main

import (
	"time"

	"github.com/arcgolabs/configx"
	"github.com/arcgolabs/dix"
	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/frontend"
	"github.com/lyonbrown4d/nespa/node"
	"github.com/spf13/pflag"
)

func addServerFlags(flags *pflag.FlagSet) {
	flags.Bool("control-enabled", true, "enable control-plane HTTP service")
	flags.String("control-addr", "127.0.0.1:7401", "control-plane HTTP listen address")
	flags.String("control-cluster-id", "local", "cluster identifier")
	flags.Duration("control-liveness-sweep-interval", 5*time.Second, "control-plane node liveness sweep interval")
	flags.Duration("control-liveness-suspect-after", 15*time.Second, "mark data nodes suspect after this heartbeat age")
	flags.Duration("control-liveness-dead-after", 30*time.Second, "mark data nodes dead after this heartbeat age")
	flags.Bool("frontend-enabled", false, "enable optional frontend webui/debug module")
	flags.String("frontend-addr", "127.0.0.1:7402", "frontend HTTP listen address")
	flags.Bool("node-enabled", true, "enable data-node TCP service")
	flags.String("node-addr", "127.0.0.1:7403", "data-node TCP listen address")
	flags.String("node-id", "node-1", "data-node identifier")
	flags.Duration("node-heartbeat-interval", 5*time.Second, "data-node control-plane heartbeat interval")
	flags.Bool("admin-enabled", true, "enable admin API module")
	flags.String("admin-addr", "127.0.0.1:7404", "admin HTTP listen address")
	flags.Uint64("node-quota-namespace-memory-bytes", 0, "default namespace memory quota for the data node; 0 disables the limit")
	flags.Uint64("node-quota-space-memory-bytes", 0, "default space memory quota for the data node; 0 disables the limit")
}

func configModule(flags *pflag.FlagSet) dix.Module {
	return dix.NewModule("config",
		dix.Providers(
			dix.Value(configSource{
				Flags:     flags,
				EnvPrefix: "NESPA",
				Defaults:  serverDefaults(),
			}),
			dix.ProviderErr1(loadServerConfig),
			dix.Provider1(controlConfigFrom),
			dix.Provider1(frontendConfigFrom),
			dix.Provider1(nodeConfigFrom),
			dix.Provider1(adminConfigFrom),
		),
	)
}

func serverDefaults() map[string]any {
	return map[string]any{
		"control.enabled":                   true,
		"control.addr":                      "127.0.0.1:7401",
		"control.cluster.id":                "local",
		"control.liveness.sweep.interval":   5 * time.Second,
		"control.liveness.suspect.after":    15 * time.Second,
		"control.liveness.dead.after":       30 * time.Second,
		"frontend.enabled":                  false,
		"frontend.addr":                     "127.0.0.1:7402",
		"node.enabled":                      true,
		"node.addr":                         "127.0.0.1:7403",
		"node.id":                           "node-1",
		"node.heartbeat.interval":           5 * time.Second,
		"admin.enabled":                     true,
		"admin.addr":                        "127.0.0.1:7404",
		"node.quota.namespace.memory.bytes": uint64(0),
		"node.quota.space.memory.bytes":     uint64(0),
	}
}

func loadServerConfig(source configSource) (serverConfig, error) {
	return configx.LoadTErr[serverConfig](
		configx.WithDefaults(source.Defaults),
		configx.WithEnvPrefix(source.EnvPrefix),
		configx.WithFlagSet(source.Flags),
	)
}

func controlConfigFrom(cfg serverConfig) control.Config {
	return control.Config{
		Addr:      cfg.Control.Addr,
		ClusterID: cfg.Control.Cluster.ID,
		Liveness: control.LivenessConfig{
			SweepInterval: cfg.Control.Liveness.Sweep.Interval,
			SuspectAfter:  cfg.Control.Liveness.Suspect.After,
			DeadAfter:     cfg.Control.Liveness.Dead.After,
		},
	}
}

func frontendConfigFrom(cfg serverConfig) frontend.Config {
	return frontend.Config{
		Addr:        cfg.Frontend.Addr,
		ControlAddr: cfg.Control.Addr,
	}
}

func nodeConfigFrom(cfg serverConfig) node.Config {
	return node.Config{
		Addr:                        cfg.Node.Addr,
		ControlAddr:                 cfg.Control.Addr,
		NodeID:                      cfg.Node.ID,
		HeartbeatInterval:           cfg.Node.Heartbeat.Interval,
		DefaultNamespaceMemoryBytes: cfg.Node.Quota.Namespace.Memory.Bytes,
		DefaultSpaceMemoryBytes:     cfg.Node.Quota.Space.Memory.Bytes,
	}
}

func adminConfigFrom(cfg serverConfig) admin.Config {
	return admin.Config{
		Addr:        cfg.Admin.Addr,
		ControlAddr: cfg.Control.Addr,
	}
}
