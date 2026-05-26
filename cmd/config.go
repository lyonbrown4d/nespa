package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/arcgolabs/configx"
	"github.com/arcgolabs/dix"
	"github.com/lyonbrown4d/nespa/admin"
	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/frontend"
	"github.com/lyonbrown4d/nespa/node"
	rediscompat "github.com/lyonbrown4d/nespa/transport/redis"
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
	flags.Bool("control-raft-join", false, "join an existing control-plane Dragonboat cluster instead of bootstrapping a new one")
	flags.StringSlice("control-raft-members", nil, "initial Dragonboat members for the control-plane cluster as node_id=raft_addr, repeated or comma-separated")
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
	flags.Bool("redis-enabled", false, "enable Redis RESP compatibility service")
	flags.String("redis-addr", "127.0.0.1:6379", "Redis RESP compatibility listen address")
	flags.StringSlice("redis-users", nil, "Redis AUTH credentials in username=password form; repeat or comma separate")
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
			dix.ProviderErr1(controlConfigFrom),
			dix.Provider1(frontendConfigFrom),
			dix.Provider1(redisConfigFrom),
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
		"control.raft.join":                    false,
		"control.raft.members":                 []string{},
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
		"redis.enabled":                        false,
		"redis.addr":                           "127.0.0.1:6379",
		"redis.users":                          []string{},
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

func parseRaftMembers(raw []string) ([]control.RaftMember, error) {
	members := make([]control.RaftMember, 0, len(raw))
	for _, item := range raw {
		member, ok, err := parseRaftMember(item)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		members = append(members, member)
	}
	return members, nil
}

func parseRaftMember(raw string) (control.RaftMember, bool, error) {
	item := strings.TrimSpace(raw)
	if item == "" {
		return control.RaftMember{}, false, nil
	}

	parts := strings.SplitN(item, "=", 2)
	if len(parts) != 2 {
		return control.RaftMember{}, false, fmt.Errorf("invalid control raft member %q, expected node_id=addr", item)
	}

	nodeID, err := parseRaftMemberNodeID(item, parts[0])
	if err != nil {
		return control.RaftMember{}, false, err
	}
	addr := strings.TrimSpace(parts[1])
	if addr == "" {
		return control.RaftMember{}, false, fmt.Errorf("invalid control raft member %q: addr is required", item)
	}

	return control.RaftMember{
		NodeID: nodeID,
		Addr:   addr,
	}, true, nil
}

func parseRaftMemberNodeID(item, rawNodeID string) (uint64, error) {
	nodeID, err := strconv.ParseUint(strings.TrimSpace(rawNodeID), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid control raft member %q: %w", item, err)
	}
	if nodeID == 0 {
		return 0, fmt.Errorf("invalid control raft member %q: node id must be > 0", item)
	}
	return nodeID, nil
}

func controlConfigFrom(cfg serverConfig) (control.Config, error) {
	members, err := parseRaftMembers(cfg.Control.Raft.Members)
	if err != nil {
		return control.Config{}, err
	}

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
			Join:            cfg.Control.Raft.Join,
			Members:         members,
			ProposalTimeout: cfg.Control.Raft.Proposal.Timeout,
		},
	}, nil
}

func frontendConfigFrom(cfg serverConfig) frontend.Config {
	return frontend.Config{
		Addr:        cfg.Frontend.Addr,
		ControlAddr: cfg.Control.Addr,
	}
}

func redisConfigFrom(cfg serverConfig) rediscompat.Config {
	return rediscompat.Config{
		Addr:  cfg.Redis.Addr,
		Users: cfg.Redis.Users,
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
