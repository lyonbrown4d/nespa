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

type taskCountConfig struct {
	Tasks int `mapstructure:"tasks"`
}

type maxConfig struct {
	Parallel taskCountConfig `mapstructure:"parallel"`
}

func addServerFlags(flags *pflag.FlagSet) {
	flags.Bool("control-enabled", true, "enable control-plane HTTP service")
	flags.String("control-addr", "127.0.0.1:7401", "control-plane HTTP listen address")
	flags.String("control-cluster-id", "local", "cluster identifier")
	flags.String("control-snapshot-path", "", "control-plane snapshot file path; empty disables file snapshot restore/save")
	flags.String("control-raft-dir", "", "Dragonboat NodeHost directory; empty uses temporary runtime storage")
	flags.String("control-raft-addr", "127.0.0.1:7601", "control-plane Dragonboat Raft listen address")
	flags.Uint64("control-raft-cluster-id", 1, "control-plane Dragonboat cluster ID")
	flags.Uint64("control-raft-node-id", 1, "control-plane Dragonboat node ID")
	flags.Duration("control-raft-proposal-timeout", 5*time.Second, "control-plane Dragonboat proposal timeout")
	flags.Duration("control-liveness-sweep-interval", 5*time.Second, "control-plane node liveness sweep interval")
	flags.Duration("control-liveness-suspect-after", 15*time.Second, "mark data nodes suspect after this heartbeat age")
	flags.Duration("control-liveness-dead-after", 30*time.Second, "mark data nodes dead after this heartbeat age")
	flags.Bool("control-migration-enabled", true, "enable control-plane migration task executor")
	flags.Duration("control-migration-sweep-interval", time.Second, "control-plane migration executor sweep interval")
	flags.Duration("control-migration-task-timeout", 10*time.Second, "control-plane migration task timeout")
	flags.Duration("control-migration-retry-interval", 2*time.Second, "control-plane migration retry interval")
	flags.Int("control-migration-max-parallel-tasks", 1, "maximum control-plane migration tasks to execute concurrently")
	flags.Bool("frontend-enabled", false, "enable optional frontend webui/debug module")
	flags.String("frontend-addr", "127.0.0.1:7402", "frontend HTTP listen address")
	flags.Bool("node-enabled", true, "enable data-node TCP service")
	flags.String("node-addr", "127.0.0.1:7403", "data-node TCP listen address")
	flags.String("node-id", "node-1", "data-node identifier")
	flags.Duration("node-heartbeat-interval", 5*time.Second, "data-node control-plane heartbeat interval")
	flags.String("node-snapshot-path", "", "data-node engine snapshot file path; empty disables file snapshot restore/save")
	flags.Duration("node-snapshot-interval", 0, "data-node engine snapshot save interval; 0 disables periodic snapshot")
	flags.String("node-replication-outbox-path", "", "data-node replication outbox JSONL path; empty disables durable outbox append")
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
		"control.enabled":                      true,
		"control.addr":                         "127.0.0.1:7401",
		"control.cluster.id":                   "local",
		"control.snapshot.path":                "",
		"control.raft.dir":                     "",
		"control.raft.addr":                    "127.0.0.1:7601",
		"control.raft.cluster.id":              uint64(1),
		"control.raft.node.id":                 uint64(1),
		"control.raft.proposal.timeout":        5 * time.Second,
		"control.liveness.sweep.interval":      5 * time.Second,
		"control.liveness.suspect.after":       15 * time.Second,
		"control.liveness.dead.after":          30 * time.Second,
		"control.migration.enabled":            true,
		"control.migration.sweep.interval":     time.Second,
		"control.migration.task.timeout":       10 * time.Second,
		"control.migration.retry.interval":     2 * time.Second,
		"control.migration.max.parallel.tasks": 1,
		"frontend.enabled":                     false,
		"frontend.addr":                        "127.0.0.1:7402",
		"node.enabled":                         true,
		"node.addr":                            "127.0.0.1:7403",
		"node.id":                              "node-1",
		"node.heartbeat.interval":              5 * time.Second,
		"node.snapshot.path":                   "",
		"node.snapshot.interval":               0,
		"node.replication.outbox.path":         "",
		"admin.enabled":                        true,
		"admin.addr":                           "127.0.0.1:7404",
		"node.quota.namespace.memory.bytes":    uint64(0),
		"node.quota.space.memory.bytes":        uint64(0),
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
		Migration: control.MigrationConfig{
			Enabled:          cfg.Control.Migration.Enabled,
			SweepInterval:    cfg.Control.Migration.Sweep.Interval,
			TaskTimeout:      cfg.Control.Migration.Task.Timeout,
			RetryBackoff:     cfg.Control.Migration.Retry.Interval,
			MaxParallelTasks: cfg.Control.Migration.Max.Parallel.Tasks,
		},
		Persistence: control.PersistenceConfig{
			SnapshotPath: cfg.Control.Snapshot.Path,
		},
		Raft: control.RaftConfig{
			NodeHostDir:     cfg.Control.Raft.Dir,
			Addr:            cfg.Control.Raft.Addr,
			ClusterID:       cfg.Control.Raft.Cluster.ID,
			NodeID:          cfg.Control.Raft.Node.ID,
			ProposalTimeout: cfg.Control.Raft.Proposal.Timeout,
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
		SnapshotPath:                cfg.Node.Snapshot.Path,
		SnapshotInterval:            cfg.Node.Snapshot.Interval,
		ReplicationOutboxPath:       cfg.Node.Replication.Outbox.Path,
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
