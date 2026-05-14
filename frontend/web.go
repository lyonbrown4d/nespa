package frontend

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	html "github.com/gofiber/template/html/v2"
)

//go:embed templates/*.html
var templateFS embed.FS

type WebServer struct {
	addr string
	app  *fiber.App
	svc  *ServiceRuntime
}

func NewWebServer(cfg Config, svc *ServiceRuntime) (*WebServer, error) {
	views, err := newTemplateEngine()
	if err != nil {
		return nil, err
	}
	server := &WebServer{
		addr: cfg.Addr,
		svc:  svc,
		app: fiber.New(fiber.Config{
			AppName: "Nespa Frontend",
			Views:   views,
		}),
	}
	server.registerRoutes()
	return server, nil
}

func (s *WebServer) Start(_ context.Context, logger *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("frontend webui starting", "addr", s.addr)
		errCh <- s.app.Listen(s.addr)
	}()

	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return fmt.Errorf("start frontend webui: %w", err)
	case <-timer.C:
		return nil
	}
}

func (s *WebServer) Stop(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- s.app.Shutdown()
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("stop frontend webui: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop frontend webui: %w", ctx.Err())
	}
}

func (s *WebServer) registerRoutes() {
	s.app.Get("/", func(c *fiber.Ctx) error {
		return c.Render("index", s.indexView())
	})
	s.app.Get("/healthz", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "service": "frontend"})
	})
	s.app.Get("/routes", func(c *fiber.Ctx) error {
		return c.JSON(s.svc.Routes())
	})
}

func (s *WebServer) indexView() fiber.Map {
	snapshot := s.svc.Routes()
	return fiber.Map{
		"Title":       "Nespa",
		"ControlAddr": s.svc.cfg.ControlAddr,
		"RouteEpoch":  snapshot.RouteEpoch,
		"RouteSource": snapshot.Source,
		"Routes":      snapshot.Routes,
	}
}

func newTemplateEngine() (*html.Engine, error) {
	templates, err := fs.Sub(templateFS, "templates")
	if err != nil {
		return nil, fmt.Errorf("load frontend templates: %w", err)
	}
	engine := html.NewFileSystem(http.FS(templates), ".html")
	if err := engine.Load(); err != nil {
		return nil, fmt.Errorf("parse frontend templates: %w", err)
	}
	return engine, nil
}
