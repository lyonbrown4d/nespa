package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arcgolabs/configx"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/logx"
	"github.com/lyonbrown4d/nespa/internal/admin"
	"github.com/lyonbrown4d/nespa/internal/control"
	"github.com/lyonbrown4d/nespa/internal/frontend"
	"github.com/lyonbrown4d/nespa/internal/node"
	"github.com/lyonbrown4d/nespa/internal/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	logger, err := logx.New(
		logx.WithConsole(true),
		logx.WithInfoLevel(),
		logx.WithLocalTime(true),
	)
	if err != nil {
		logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := rootCommand(ctx, stdout, logger)
	cmd.SetArgs(args)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func rootCommand(ctx context.Context, stdout io.Writer, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "nespa",
		Short:         "Nespa distributed cache platform",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		devCommand(ctx, stdout, logger),
		controlCommand(ctx, logger),
		frontendCommand(ctx, logger),
		nodeCommand(ctx, logger),
		adminCommand(ctx, logger),
		cliCommand(stdout),
		versionCommand(stdout),
	)

	return cmd
}

func versionCommand(stdout io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		RunE: func(*cobra.Command, []string) error {
			fmt.Fprintf(stdout, "nespa %s\n", version)
			return nil
		},
	}
}

func cliCommand(stdout io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "cli",
		Short: "Run the placeholder admin CLI",
		RunE: func(*cobra.Command, []string) error {
			fmt.Fprintln(stdout, "nespa cli is ready; admin commands will be added as the control plane lands.")
			return nil
		},
	}
}

type endpointConfig struct {
	Addr string `mapstructure:"addr"`
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

type controlConfig struct {
	Addr     string                `mapstructure:"addr"`
	Liveness controlLivenessConfig `mapstructure:"liveness"`
}

type nodeConfig struct {
	Addr      string          `mapstructure:"addr"`
	Heartbeat heartbeatConfig `mapstructure:"heartbeat"`
	Quota     quotaConfig     `mapstructure:"quota"`
}

type heartbeatConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

type frontendConfig struct {
	Addr string         `mapstructure:"addr"`
	Node endpointConfig `mapstructure:"node"`
}

type devConfig struct {
	Control  controlConfig  `mapstructure:"control"`
	Frontend frontendConfig `mapstructure:"frontend"`
	Node     nodeConfig     `mapstructure:"node"`
	Admin    endpointConfig `mapstructure:"admin"`
}

type endpointInfo struct {
	name string
	addr string
}

func devCommand(ctx context.Context, stdout io.Writer, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Run control, frontend, data node, and admin services in one process",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig[devConfig](cmd.Flags(), "NESPA", map[string]any{
				"control.addr":                      "127.0.0.1:7401",
				"control.liveness.sweep.interval":   5 * time.Second,
				"control.liveness.suspect.after":    15 * time.Second,
				"control.liveness.dead.after":       30 * time.Second,
				"frontend.addr":                     "127.0.0.1:7402",
				"node.addr":                         "127.0.0.1:7403",
				"node.heartbeat.interval":           5 * time.Second,
				"admin.addr":                        "127.0.0.1:7404",
				"node.quota.namespace.memory.bytes": uint64(0),
				"node.quota.space.memory.bytes":     uint64(0),
			})
			if err != nil {
				return fmt.Errorf("load dev config: %w", err)
			}

			frontendNodeAddr := cfg.Frontend.Node.Addr
			if frontendNodeAddr == "" {
				frontendNodeAddr = cfg.Node.Addr
			}

			modules := []dix.Module{
				control.Module(control.Config{
					Addr:      cfg.Control.Addr,
					ClusterID: "dev",
					Liveness: control.LivenessConfig{
						SweepInterval: cfg.Control.Liveness.Sweep.Interval,
						SuspectAfter:  cfg.Control.Liveness.Suspect.After,
						DeadAfter:     cfg.Control.Liveness.Dead.After,
					},
				}),
				frontend.Module(frontend.Config{
					Addr:        cfg.Frontend.Addr,
					ControlAddr: cfg.Control.Addr,
					NodeAddr:    frontendNodeAddr,
				}),
				node.Module(node.Config{
					Addr:                        cfg.Node.Addr,
					ControlAddr:                 cfg.Control.Addr,
					NodeID:                      "dev-node-1",
					HeartbeatInterval:           cfg.Node.Heartbeat.Interval,
					DefaultNamespaceMemoryBytes: cfg.Node.Quota.Namespace.Memory.Bytes,
					DefaultSpaceMemoryBytes:     cfg.Node.Quota.Space.Memory.Bytes,
				}),
				admin.Module(admin.Config{Addr: cfg.Admin.Addr, ControlAddr: cfg.Control.Addr}),
			}

			fmt.Fprintln(stdout, "nespa dev starting")
			for _, endpoint := range []endpointInfo{
				{name: "control", addr: cfg.Control.Addr},
				{name: "frontend", addr: cfg.Frontend.Addr},
				{name: "node", addr: cfg.Node.Addr},
				{name: "admin", addr: cfg.Admin.Addr},
			} {
				fmt.Fprintf(stdout, "  %-10s http://%s\n", endpoint.name, endpoint.addr)
			}

			return runDixApp(ctx, logger, "nespa-dev", modules...)
		},
	}

	cmd.Flags().String("control-addr", "127.0.0.1:7401", "control-plane HTTP listen address")
	cmd.Flags().Duration("control-liveness-sweep-interval", 5*time.Second, "control-plane node liveness sweep interval")
	cmd.Flags().Duration("control-liveness-suspect-after", 15*time.Second, "mark data nodes suspect after this heartbeat age")
	cmd.Flags().Duration("control-liveness-dead-after", 30*time.Second, "mark data nodes dead after this heartbeat age")
	cmd.Flags().String("frontend-addr", "127.0.0.1:7402", "frontend HTTP listen address")
	cmd.Flags().String("frontend-node-addr", "", "data-node address used by the frontend gateway; defaults to --node-addr")
	cmd.Flags().String("node-addr", "127.0.0.1:7403", "data-node HTTP listen address")
	cmd.Flags().Duration("node-heartbeat-interval", 5*time.Second, "data-node control-plane heartbeat interval")
	cmd.Flags().String("admin-addr", "127.0.0.1:7404", "admin HTTP listen address")
	cmd.Flags().Uint64("node-quota-namespace-memory-bytes", 0, "default namespace memory quota for the dev data node; 0 disables the limit")
	cmd.Flags().Uint64("node-quota-space-memory-bytes", 0, "default space memory quota for the dev data node; 0 disables the limit")
	return cmd
}

type controlCommandConfig struct {
	Addr     string                `mapstructure:"addr"`
	Cluster  identityConfig        `mapstructure:"cluster"`
	Liveness controlLivenessConfig `mapstructure:"liveness"`
}

func controlCommand(ctx context.Context, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "control",
		Short: "Run the control-plane service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig[controlCommandConfig](cmd.Flags(), "NESPA_CONTROL", map[string]any{
				"addr":                    "127.0.0.1:7401",
				"cluster.id":              "local",
				"liveness.sweep.interval": 5 * time.Second,
				"liveness.suspect.after":  15 * time.Second,
				"liveness.dead.after":     30 * time.Second,
			})
			if err != nil {
				return fmt.Errorf("load control config: %w", err)
			}

			return runDixApp(ctx, logger, "nespa-control", control.Module(control.Config{
				Addr:      cfg.Addr,
				ClusterID: cfg.Cluster.ID,
				Liveness: control.LivenessConfig{
					SweepInterval: cfg.Liveness.Sweep.Interval,
					SuspectAfter:  cfg.Liveness.Suspect.After,
					DeadAfter:     cfg.Liveness.Dead.After,
				},
			}))
		},
	}

	cmd.Flags().String("addr", "127.0.0.1:7401", "HTTP listen address")
	cmd.Flags().String("cluster-id", "local", "cluster identifier")
	cmd.Flags().Duration("liveness-sweep-interval", 5*time.Second, "node liveness sweep interval")
	cmd.Flags().Duration("liveness-suspect-after", 15*time.Second, "mark data nodes suspect after this heartbeat age")
	cmd.Flags().Duration("liveness-dead-after", 30*time.Second, "mark data nodes dead after this heartbeat age")
	return cmd
}

type frontendCommandConfig struct {
	Addr    string         `mapstructure:"addr"`
	Control endpointConfig `mapstructure:"control"`
	Node    endpointConfig `mapstructure:"node"`
}

func frontendCommand(ctx context.Context, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "frontend",
		Short: "Run the frontend gateway service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig[frontendCommandConfig](cmd.Flags(), "NESPA_FRONTEND", map[string]any{
				"addr":         "127.0.0.1:7402",
				"control.addr": "127.0.0.1:7401",
				"node.addr":    "127.0.0.1:7403",
			})
			if err != nil {
				return fmt.Errorf("load frontend config: %w", err)
			}

			return runDixApp(ctx, logger, "nespa-frontend", frontend.Module(frontend.Config{
				Addr:        cfg.Addr,
				ControlAddr: cfg.Control.Addr,
				NodeAddr:    cfg.Node.Addr,
			}))
		},
	}

	cmd.Flags().String("addr", "127.0.0.1:7402", "HTTP listen address")
	cmd.Flags().String("control-addr", "127.0.0.1:7401", "control-plane address")
	cmd.Flags().String("node-addr", "127.0.0.1:7403", "data-node address")
	return cmd
}

type nodeCommandConfig struct {
	Addr      string          `mapstructure:"addr"`
	Control   endpointConfig  `mapstructure:"control"`
	Node      identityConfig  `mapstructure:"node"`
	Heartbeat heartbeatConfig `mapstructure:"heartbeat"`
	Quota     quotaConfig     `mapstructure:"quota"`
}

func nodeCommand(ctx context.Context, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Run a data-node service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig[nodeCommandConfig](cmd.Flags(), "NESPA_NODE", map[string]any{
				"addr":                         "127.0.0.1:7403",
				"control.addr":                 "127.0.0.1:7401",
				"node.id":                      "node-1",
				"heartbeat.interval":           5 * time.Second,
				"quota.namespace.memory.bytes": uint64(0),
				"quota.space.memory.bytes":     uint64(0),
			})
			if err != nil {
				return fmt.Errorf("load node config: %w", err)
			}

			return runDixApp(ctx, logger, "nespa-node", node.Module(node.Config{
				Addr:                        cfg.Addr,
				ControlAddr:                 cfg.Control.Addr,
				NodeID:                      cfg.Node.ID,
				HeartbeatInterval:           cfg.Heartbeat.Interval,
				DefaultNamespaceMemoryBytes: cfg.Quota.Namespace.Memory.Bytes,
				DefaultSpaceMemoryBytes:     cfg.Quota.Space.Memory.Bytes,
			}))
		},
	}

	cmd.Flags().String("addr", "127.0.0.1:7403", "HTTP listen address")
	cmd.Flags().String("control-addr", "127.0.0.1:7401", "control-plane address")
	cmd.Flags().String("node-id", "node-1", "data-node identifier")
	cmd.Flags().Duration("heartbeat-interval", 5*time.Second, "control-plane heartbeat interval")
	cmd.Flags().Uint64("quota-namespace-memory-bytes", 0, "default namespace memory quota; 0 disables the limit")
	cmd.Flags().Uint64("quota-space-memory-bytes", 0, "default space memory quota; 0 disables the limit")
	return cmd
}

type adminCommandConfig struct {
	Addr    string         `mapstructure:"addr"`
	Control endpointConfig `mapstructure:"control"`
}

func adminCommand(ctx context.Context, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Run the admin API service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig[adminCommandConfig](cmd.Flags(), "NESPA_ADMIN", map[string]any{
				"addr":         "127.0.0.1:7404",
				"control.addr": "127.0.0.1:7401",
			})
			if err != nil {
				return fmt.Errorf("load admin config: %w", err)
			}

			return runDixApp(ctx, logger, "nespa-admin", admin.Module(admin.Config{
				Addr:        cfg.Addr,
				ControlAddr: cfg.Control.Addr,
			}))
		},
	}

	cmd.Flags().String("addr", "127.0.0.1:7404", "HTTP listen address")
	cmd.Flags().String("control-addr", "127.0.0.1:7401", "control-plane address")
	return cmd
}

func loadConfig[T any](flags *pflag.FlagSet, envPrefix string, defaults map[string]any) (T, error) {
	return configx.LoadTErr[T](
		configx.WithDefaults(defaults),
		configx.WithEnvPrefix(envPrefix),
		configx.WithFlagSet(flags),
	)
}

func runDixApp(ctx context.Context, logger *slog.Logger, name string, modules ...dix.Module) error {
	all := make([]dix.Module, 0, len(modules)+1)
	all = append(all, runtime.FoundationModule(logger))
	all = append(all, modules...)

	app := dix.New(name,
		dix.WithLogger(logger),
		dix.WithRecentEvents(128),
		dix.WithModules(all...),
		dix.WithRunStopTimeout(5*time.Second),
	)

	return app.RunContext(ctx)
}
