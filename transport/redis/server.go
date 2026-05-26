package redis

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/lyonbrown4d/nespa/cache"
)

const (
	defaultAddr        = "127.0.0.1:6379"
	defaultSpaceSuffix = "-space"
)

type Config struct {
	Addr  string
	Users []string
}

type Server struct {
	addr        string
	service     cache.Service
	credentials map[string]string

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
}

type session struct {
	authenticated bool
	namespace     string
	db            int
	protocol      int
	close         bool
	inTransaction bool
	queued        []queuedCommand
	txService     cache.Service
}

type queuedCommand []respArg

func NewServer(cfg Config, service cache.Service) *Server {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		addr = defaultAddr
	}
	return &Server{
		addr:        addr,
		service:     service,
		credentials: parseCredentials(cfg.Users),
	}
}

func (s *Server) Start(ctx context.Context, logger *slog.Logger) error {
	if len(s.credentials) == 0 {
		return errors.New("start redis compatibility server: no AUTH users configured")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen redis compatibility server: %w", err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	logger.Info("redis compatibility server starting", "addr", s.addr)
	s.wg.Go(func() {
		s.acceptLoop(ctx, logger, listener)
	})
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.mu.Unlock()

	if listener != nil {
		if err := listener.Close(); err != nil {
			return fmt.Errorf("close redis compatibility listener: %w", err)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop redis compatibility server: %w", ctx.Err())
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) acceptLoop(ctx context.Context, logger *slog.Logger, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Warn("redis compatibility accept failed", "error", err)
			continue
		}

		s.wg.Go(func() {
			s.serveConn(ctx, logger, conn)
		})
	}
}

func (s *Server) serveConn(ctx context.Context, logger *slog.Logger, conn net.Conn) {
	defer closeRedisConn(conn)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	state := &session{db: 0, protocol: 2}
	s.runSession(ctx, logger, reader, writer, state)
}

func (s *Server) runSession(
	ctx context.Context,
	logger *slog.Logger,
	reader *bufio.Reader,
	writer *bufio.Writer,
	state *session,
) {
	for {
		args, err := readRESPCommand(reader)
		if s.sessionReadFailed(logger, err) {
			return
		}
		if s.writeCommandResponse(ctx, logger, writer, state, args) {
			return
		}
	}
}

func (s *Server) sessionReadFailed(logger *slog.Logger, err error) bool {
	if err == nil {
		return false
	}
	if !errors.Is(err, io.EOF) {
		logger.Debug("redis compatibility frame decode failed", "error", err)
	}
	return true
}

func (s *Server) writeCommandResponse(
	ctx context.Context,
	logger *slog.Logger,
	writer *bufio.Writer,
	state *session,
	args []respArg,
) bool {
	response := s.responseForCommand(ctx, state, args)
	if err := writeRESP(writer, response); err != nil {
		logger.Debug("redis compatibility frame encode failed", "error", err)
		return true
	}
	return state.close
}

func (s *Server) responseForCommand(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) == 0 {
		return errorString("ERR empty command")
	}
	return s.handleCommand(ctx, state, args)
}

func (state *session) activeService(server *Server) cache.Service {
	if state.txService != nil {
		return state.txService
	}
	return server.service
}

func parseCredentials(items []string) map[string]string {
	out := make(map[string]string)
	for index := range items {
		raw := strings.TrimSpace(items[index])
		if raw == "" {
			continue
		}
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 {
			continue
		}
		user := strings.TrimSpace(parts[0])
		password := parts[1]
		if validUsername(user) && password != "" {
			out[user] = password
		}
	}
	return out
}

func validUsername(user string) bool {
	if user == "" {
		return false
	}
	for _, r := range user {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func (s *Server) authenticate(user, password string) bool {
	want, ok := s.credentials[user]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(password)) == 1
}

func (s *Server) spaceName(db int) string {
	return strconv.Itoa(db) + defaultSpaceSuffix
}

func closeRedisConn(conn interface{ Close() error }) {
	if err := conn.Close(); err != nil {
		return
	}
}
