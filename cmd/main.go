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
	"github.com/samber/oops"
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
			}, dix.LifecycleName("server.banner.print")),
		),
	)
}

func endpoints(cfg serverConfig) *collectionlist.List[endpointInfo] {
	items := collectionlist.NewList[endpointInfo]()

	addEndpoint(items, cfg.Control.Enabled, "control", "http", cfg.Control.Addr)
	addEndpoint(items, cfg.Node.Enabled, "node", "tcp", cfg.Node.Addr)
	addEndpoint(items, cfg.Frontend.Enabled, "frontend", "http", cfg.Frontend.Addr)
	addEndpoint(items, cfg.Redis.Enabled, "redis", "redis", cfg.Redis.Addr)
	addEndpoint(items, cfg.Admin.Enabled, "admin", "http", cfg.Admin.Addr)

	return items
}

func addEndpoint(items *collectionlist.List[endpointInfo], enabled bool, name, scheme, addr string) {
	if enabled && addr != "" {
		items.Add(endpointInfo{name: name, scheme: scheme, addr: addr})
	}
}

func runDixApp(ctx context.Context, stdout io.Writer, logger *slog.Logger, flags *pflag.FlagSet) error {
	cfg, err := loadServerConfig(configSource{
		Flags:     flags,
		EnvPrefix: "NESPA",
		Defaults:  serverDefaults(),
	})
	if err != nil {
		return fmt.Errorf("load server config: %w", err)
	}
	if err := validateServerConfig(cfg); err != nil {
		return err
	}

	modules := collectionlist.NewList[dix.Module](
		foundationModule(logger),
		configModule(flags),
		bannerModule(stdout),
		controlModule(cfg.Control.Enabled),
		nodeModule(cfg.Node.Enabled),
		frontendModule(cfg.Frontend.Enabled),
		redisModule(cfg.Redis.Enabled),
		adminModule(cfg.Admin.Enabled),
	)

	app := dix.New("nespa",
		dix.WithLogger(logger),
		dix.WithRecentEvents(128),
		dix.WithModules(modules.Values()...),
		dix.WithRunStopTimeout(5*time.Second),
	)

	if err := app.RunContext(ctx); err != nil {
		return fmt.Errorf("run nespa app: %w", err)
	}
	return nil
}

func validateServerConfig(cfg serverConfig) error {
	if !cfg.Control.Enabled && !cfg.Node.Enabled && !cfg.Frontend.Enabled && !cfg.Redis.Enabled && !cfg.Admin.Enabled {
		return fmt.Errorf("validate server config: %w",
			oops.Code("invalid_server_config").
				In("cmd").
				With(
					"control_enabled", cfg.Control.Enabled,
					"node_enabled", cfg.Node.Enabled,
					"frontend_enabled", cfg.Frontend.Enabled,
					"redis_enabled", cfg.Redis.Enabled,
					"admin_enabled", cfg.Admin.Enabled,
				).
				New("at least one service must be enabled"))
	}
	if err := validateAdminConfig(cfg); err != nil {
		return err
	}
	return validateRedisConfig(cfg)
}

func validateAdminConfig(cfg serverConfig) error {
	if cfg.Admin.Enabled && (!cfg.Control.Enabled || !cfg.Node.Enabled) {
		return fmt.Errorf("validate server config: %w",
			oops.Code("invalid_server_config").
				In("cmd").
				With("admin_enabled", cfg.Admin.Enabled, "control_enabled", cfg.Control.Enabled, "node_enabled", cfg.Node.Enabled).
				New("admin service requires colocated control and node services"))
	}
	return nil
}

func validateRedisConfig(cfg serverConfig) error {
	if cfg.Redis.Enabled && !cfg.Node.Enabled {
		return fmt.Errorf("validate server config: %w",
			oops.Code("invalid_server_config").
				In("cmd").
				With("redis_enabled", cfg.Redis.Enabled, "node_enabled", cfg.Node.Enabled).
				New("redis compatibility service requires node service"))
	}
	if cfg.Redis.Enabled && len(cfg.Redis.Users) == 0 {
		return fmt.Errorf("validate server config: %w",
			oops.Code("invalid_server_config").
				In("cmd").
				With("redis_enabled", cfg.Redis.Enabled).
				New("redis compatibility service requires at least one AUTH user"))
	}
	return nil
}
