package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/httpx"
)

type HTTPConfig struct {
	Name     string
	Addr     string
	Metadata map[string]string
	Routes   func(httpx.ServerRuntime)
}

type HTTPService struct {
	name     string
	addr     string
	metadata map[string]string
	routes   func(httpx.ServerRuntime)

	mu     sync.Mutex
	server httpx.ServerRuntime
	errCh  chan error
}

func NewHTTPService(cfg HTTPConfig) *HTTPService {
	metadata := make(map[string]string, len(cfg.Metadata))
	maps.Copy(metadata, cfg.Metadata)

	return &HTTPService{
		name:     cfg.Name,
		addr:     cfg.Addr,
		metadata: metadata,
		routes:   cfg.Routes,
	}
}

func (s *HTTPService) Name() string {
	return s.name
}

func (s *HTTPService) Addr() string {
	return s.addr
}

func (s *HTTPService) Start(ctx context.Context, logger *slog.Logger, bus eventx.BusRuntime) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		return nil
	}

	server := httpx.New(
		httpx.WithLogger(logger),
		httpx.WithOpenAPIInfo("Nespa "+s.name, "dev", "Nespa "+s.name+" service"),
		httpx.WithPanicRecover(true),
		httpx.WithAccessLog(false),
	)

	s.registerBaseRoutes(server)
	if s.routes != nil {
		s.routes(server)
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("service starting", "service", s.name, "addr", s.addr)
		if err := bus.Publish(ctx, ServiceEvent{Service: s.name, Addr: s.addr, State: "starting"}); err != nil {
			logger.Warn("service start event failed", "service", s.name, "error", err)
		}
		if err := server.ListenAndServeContext(ctx, s.addr); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return fmt.Errorf("%s start: %w", s.name, err)
	case <-timer.C:
		s.server = server
		s.errCh = errCh
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s start canceled: %w", s.name, ctx.Err())
	}
}

func (s *HTTPService) Stop(ctx context.Context, logger *slog.Logger, bus eventx.BusRuntime) error {
	s.mu.Lock()
	server := s.server
	errCh := s.errCh
	s.server = nil
	s.errCh = nil
	s.mu.Unlock()

	if server == nil {
		return nil
	}

	if err := server.Shutdown(); err != nil {
		return fmt.Errorf("%s shutdown: %w", s.name, err)
	}

	select {
	case <-errCh:
	case <-ctx.Done():
		return fmt.Errorf("%s stop canceled: %w", s.name, ctx.Err())
	}

	logger.Info("service stopped", "service", s.name)
	if err := bus.Publish(ctx, ServiceEvent{Service: s.name, Addr: s.addr, State: "stopped"}); err != nil {
		logger.Warn("service stop event failed", "service", s.name, "error", err)
	}
	return nil
}

type HealthBody struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type VersionBody struct {
	Service string `json:"service"`
	Version string `json:"version"`
}

type ConfigBody struct {
	Service  string            `json:"service"`
	Addr     string            `json:"addr"`
	Metadata map[string]string `json:"metadata"`
}

type ServiceEvent struct {
	Service string
	Addr    string
	State   string
}

func (e ServiceEvent) Name() string {
	return "nespa.service." + e.State
}

func (s *HTTPService) registerBaseRoutes(server httpx.ServerRuntime) {
	httpx.MustGet(server, "/healthz", func(context.Context, *EmptyInput) (*JSONResponse[HealthBody], error) {
		return JSON(HealthBody{Status: "ok", Service: s.name}), nil
	})

	httpx.MustGet(server, "/readyz", func(context.Context, *EmptyInput) (*JSONResponse[HealthBody], error) {
		return JSON(HealthBody{Status: "ready", Service: s.name}), nil
	})

	httpx.MustGet(server, "/version", func(context.Context, *EmptyInput) (*JSONResponse[VersionBody], error) {
		return JSON(VersionBody{Service: s.name, Version: "dev"}), nil
	})

	httpx.MustGet(server, "/debug/config", func(context.Context, *EmptyInput) (*JSONResponse[ConfigBody], error) {
		return JSON(ConfigBody{
			Service:  s.name,
			Addr:     s.addr,
			Metadata: s.metadata,
		}), nil
	})
}
