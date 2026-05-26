package main

import (
	"time"

	"github.com/spf13/pflag"
)

type endpointConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Addr    string `mapstructure:"addr"`
}

type memoryConfig struct {
	Bytes uint64 `mapstructure:"bytes"`
}

type quotaScopeConfig struct {
	Memory memoryConfig `mapstructure:"memory"`
}

type quotaConfig struct {
	Namespace quotaScopeConfig `mapstructure:"namespace"`
	Space     quotaScopeConfig `mapstructure:"space"`
}

type identityConfig struct {
	ID string `mapstructure:"id"`
}

type numericIdentityConfig struct {
	ID uint64 `mapstructure:"id"`
}

type snapshotConfig struct {
	Path     string        `mapstructure:"path"`
	Interval time.Duration `mapstructure:"interval"`
}

type outboxConfig struct {
	Path string `mapstructure:"path"`
}

type replicationConfig struct {
	Outbox outboxConfig `mapstructure:"outbox"`
}

type timeoutConfig struct {
	Timeout time.Duration `mapstructure:"timeout"`
}

type afterConfig struct {
	After time.Duration `mapstructure:"after"`
}

type intervalConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

type controlLivenessConfig struct {
	Sweep   intervalConfig `mapstructure:"sweep"`
	Suspect afterConfig    `mapstructure:"suspect"`
	Dead    afterConfig    `mapstructure:"dead"`
}

type controlMigrationConfig struct {
	Enabled bool           `mapstructure:"enabled"`
	Sweep   intervalConfig `mapstructure:"sweep"`
	Task    timeoutConfig  `mapstructure:"task"`
	Retry   intervalConfig `mapstructure:"retry"`
	Max     maxConfig      `mapstructure:"max"`
}

type controlRaftConfig struct {
	Dir      string                `mapstructure:"dir"`
	Addr     string                `mapstructure:"addr"`
	Cluster  numericIdentityConfig `mapstructure:"cluster"`
	Node     numericIdentityConfig `mapstructure:"node"`
	Join     bool                  `mapstructure:"join"`
	Members  []string              `mapstructure:"members"`
	Proposal timeoutConfig         `mapstructure:"proposal"`
}

type controlConfig struct {
	Enabled   bool                   `mapstructure:"enabled"`
	Addr      string                 `mapstructure:"addr"`
	Cluster   identityConfig         `mapstructure:"cluster"`
	Snapshot  snapshotConfig         `mapstructure:"snapshot"`
	Raft      controlRaftConfig      `mapstructure:"raft"`
	Liveness  controlLivenessConfig  `mapstructure:"liveness"`
	Migration controlMigrationConfig `mapstructure:"migration"`
}

type nodeConfig struct {
	Enabled     bool              `mapstructure:"enabled"`
	Addr        string            `mapstructure:"addr"`
	ID          string            `mapstructure:"id"`
	Heartbeat   heartbeatConfig   `mapstructure:"heartbeat"`
	Snapshot    snapshotConfig    `mapstructure:"snapshot"`
	Replication replicationConfig `mapstructure:"replication"`
	Quota       quotaConfig       `mapstructure:"quota"`
}

type heartbeatConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

type frontendConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Addr    string `mapstructure:"addr"`
}

type redisConfig struct {
	Enabled bool     `mapstructure:"enabled"`
	Addr    string   `mapstructure:"addr"`
	Users   []string `mapstructure:"users"`
}

type serverConfig struct {
	Control  controlConfig  `mapstructure:"control"`
	Frontend frontendConfig `mapstructure:"frontend"`
	Redis    redisConfig    `mapstructure:"redis"`
	Node     nodeConfig     `mapstructure:"node"`
	Admin    endpointConfig `mapstructure:"admin"`
}

type configSource struct {
	Flags     *pflag.FlagSet
	EnvPrefix string
	Defaults  map[string]any
}

type endpointInfo struct {
	name   string
	scheme string
	addr   string
}
