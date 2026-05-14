// Package main wires the Nespa server binary.
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

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/logx"
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
		if _, printErr := fmt.Fprintln(stderr, err); printErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func rootCommand(ctx context.Context, stdout io.Writer, logger *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "nespa",
		Short:         "Run the Nespa server",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDixApp(ctx, stdout, logger, cmd.Flags())
		},
	}

	addServerFlags(cmd.Flags())
	cmd.AddCommand(versionCommand(stdout))
	return cmd
}

func versionCommand(stdout io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		RunE: func(*cobra.Command, []string) error {
			_, err := fmt.Fprintf(stdout, "nespa %s\n", version)
			if err != nil {
				return fmt.Errorf("write version: %w", err)
			}
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
	Cluster  identityConfig        `mapstructure:"cluster"`
	Liveness controlLivenessConfig `mapstructure:"liveness"`
}

type nodeConfig struct {
	Addr      string          `mapstructure:"addr"`
	ID        string          `mapstructure:"id"`
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

type serverConfig struct {
	Control  controlConfig  `mapstructure:"control"`
	Frontend frontendConfig `mapstructure:"frontend"`
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

func bannerModule(stdout io.Writer) dix.Module {
	return dix.NewModule("server.banner",
		dix.Hooks(
			dix.OnStart[serverConfig](func(_ context.Context, cfg serverConfig) error {
				if _, err := fmt.Fprintln(stdout, "nespa server starting"); err != nil {
					return fmt.Errorf("write startup banner: %w", err)
				}
				var writeErr error
				endpoints(cfg).Range(func(_ int, endpoint endpointInfo) bool {
					_, writeErr = fmt.Fprintf(stdout, "  %-10s %s://%s\n", endpoint.name, endpoint.scheme, endpoint.addr)
					return writeErr == nil
				})
				if writeErr != nil {
					return fmt.Errorf("write startup endpoint: %w", writeErr)
				}
				return nil
			}, dix.LifecycleName("server.banner.print"), dix.LifecycleBefore("control.http.start")),
		),
	)
}

func endpoints(cfg serverConfig) *collectionlist.List[endpointInfo] {
	return collectionlist.NewList(
		endpointInfo{name: "control", scheme: "http", addr: cfg.Control.Addr},
		endpointInfo{name: "frontend", scheme: "http", addr: cfg.Frontend.Addr},
		endpointInfo{name: "node", scheme: "tcp", addr: cfg.Node.Addr},
		endpointInfo{name: "admin", scheme: "http", addr: cfg.Admin.Addr},
	)
}

func runDixApp(ctx context.Context, stdout io.Writer, logger *slog.Logger, flags *pflag.FlagSet) error {
	app := dix.New("nespa",
		dix.WithLogger(logger),
		dix.WithRecentEvents(128),
		dix.WithModules(
			foundationModule(logger),
			configModule(flags),
			bannerModule(stdout),
			controlModule(),
			frontendModule(),
			nodeModule(),
			adminModule(),
		),
		dix.WithRunStopTimeout(5*time.Second),
	)

	if err := app.RunContext(ctx); err != nil {
		return fmt.Errorf("run nespa app: %w", err)
	}
	return nil
}
