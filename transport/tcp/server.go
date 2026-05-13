package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cache/engine"
	"github.com/lyonbrown4d/nespa/cacheapi"
	"github.com/lyonbrown4d/nespa/protocol"
)

type ServerConfig struct {
	Addr string
}

type Server struct {
	addr    string
	service cache.Service
	codec   *protocol.Codec

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
}

func NewServer(cfg ServerConfig, service cache.Service) *Server {
	return &Server{
		addr:    cfg.Addr,
		service: service,
		codec:   protocol.NewCodec(),
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

func (s *Server) Start(ctx context.Context, logger *slog.Logger) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen cache tcp server: %w", err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	logger.Info("cache tcp server starting", "addr", s.addr)
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
			return fmt.Errorf("close cache tcp listener: %w", err)
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
		return fmt.Errorf("stop cache tcp server: %w", ctx.Err())
	}
}

func (s *Server) acceptLoop(ctx context.Context, logger *slog.Logger, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Warn("cache tcp accept failed", "error", err)
			continue
		}
		s.wg.Go(func() {
			s.serveConn(ctx, logger, conn)
		})
	}
}

func (s *Server) serveConn(ctx context.Context, logger *slog.Logger, conn net.Conn) {
	defer closeConn(conn)
	for {
		frame, err := s.codec.Decode(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Debug("cache tcp frame decode failed", "error", err)
			}
			return
		}
		if err := s.codec.Encode(conn, s.handleFrame(ctx, frame)); err != nil {
			logger.Debug("cache tcp frame encode failed", "error", err)
			return
		}
	}
}

func (s *Server) handleFrame(ctx context.Context, frame protocol.Frame) protocol.Frame {
	switch frame.Op {
	case protocol.OpCacheSet:
		return s.handleSet(ctx, frame)
	case protocol.OpCacheGet:
		return s.handleGet(ctx, frame)
	case protocol.OpCacheDelete:
		return s.handleDelete(ctx, frame)
	case protocol.OpCacheBatchGet, protocol.OpCacheBatchSet, protocol.OpNodeHeartbeat, protocol.OpControlSnapshot, protocol.OpControlWatch:
		return errorFrame(frame, "unsupported_op", fmt.Errorf("unsupported cache op %d", frame.Op))
	default:
		return errorFrame(frame, "unsupported_op", fmt.Errorf("unsupported cache op %d", frame.Op))
	}
}

func (s *Server) handleSet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var body cacheapi.SetBody
	if err := json.Unmarshal(frame.Metadata, &body); err != nil {
		return errorFrame(frame, "bad_metadata", err)
	}
	value := frame.Payload
	if len(value) == 0 {
		value = []byte(body.Value)
	}
	rec, err := s.service.Set(ctx, keyFromSet(body), value, cache.SetOptions{
		TTL:              ttlFromMillis(body.TTLMillis),
		NamespaceVersion: body.NamespaceVersion,
		SpaceVersion:     body.SpaceVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return recordFrame(frame, rec, true)
}

func (s *Server) handleGet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var input cacheapi.GetInput
	if err := json.Unmarshal(frame.Metadata, &input); err != nil {
		return errorFrame(frame, "bad_metadata", err)
	}
	rec, found, err := s.service.Get(ctx, keyFromGet(input), cache.GetOptions{
		NamespaceVersion: input.NamespaceVersion,
		SpaceVersion:     input.SpaceVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return recordFrame(frame, rec, found)
}

func (s *Server) handleDelete(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var input cacheapi.DeleteInput
	if err := json.Unmarshal(frame.Metadata, &input); err != nil {
		return errorFrame(frame, "bad_metadata", err)
	}
	deleted, err := s.service.Delete(ctx, keyFromDelete(input))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return jsonFrame(frame, cacheapi.DeleteBody{Deleted: deleted}, nil)
}

func recordFrame(frame protocol.Frame, rec cache.Record, found bool) protocol.Frame {
	if !found {
		return jsonFrame(frame, cacheapi.RecordBody{Found: false}, nil)
	}
	body := cacheapi.RecordBody{
		Found:            true,
		Namespace:        rec.Key.Namespace,
		Space:            rec.Key.Space,
		Entity:           rec.Key.Entity,
		Key:              rec.Key.Key,
		Version:          rec.Version,
		NamespaceVersion: rec.NamespaceVersion,
		SpaceVersion:     rec.SpaceVersion,
	}
	if !rec.ExpireAt.IsZero() {
		body.ExpireAtUnixMs = rec.ExpireAt.UnixMilli()
	}
	return jsonFrame(frame, body, rec.Value)
}

func jsonFrame(request protocol.Frame, metadata any, payload []byte) protocol.Frame {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return errorFrame(request, "encode_metadata", err)
	}
	return protocol.Frame{
		Flags:      protocol.FlagResponse,
		Op:         request.Op,
		RequestID:  request.RequestID,
		RouteEpoch: request.RouteEpoch,
		Metadata:   raw,
		Payload:    payload,
	}
}

func cacheErrorFrame(request protocol.Frame, err error) protocol.Frame {
	switch {
	case errors.Is(err, cache.ErrQuotaExceeded):
		return errorFrame(request, "quota_exceeded", err)
	case errors.Is(err, engine.ErrInvalidKey):
		return errorFrame(request, "invalid_key", err)
	default:
		return errorFrame(request, "internal", err)
	}
}

func errorFrame(request protocol.Frame, code string, err error) protocol.Frame {
	raw, marshalErr := json.Marshal(cacheapi.ErrorBody{Code: code, Message: err.Error()})
	if marshalErr != nil {
		raw = []byte(`{"code":"internal","message":"cache tcp error"}`)
	}
	return protocol.Frame{
		Flags:      protocol.FlagResponse | protocol.FlagError,
		Op:         request.Op,
		RequestID:  request.RequestID,
		RouteEpoch: request.RouteEpoch,
		Metadata:   raw,
	}
}

func keyFromSet(body cacheapi.SetBody) cache.Key {
	return cache.Key{Namespace: body.Namespace, Space: body.Space, Entity: body.Entity, Key: body.Key}
}

func keyFromGet(input cacheapi.GetInput) cache.Key {
	return cache.Key{Namespace: input.Namespace, Space: input.Space, Entity: input.Entity, Key: input.Key}
}

func keyFromDelete(input cacheapi.DeleteInput) cache.Key {
	return cache.Key{Namespace: input.Namespace, Space: input.Space, Entity: input.Entity, Key: input.Key}
}

func ttlFromMillis(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}
