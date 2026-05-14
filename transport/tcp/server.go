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

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

type ServerConfig struct {
	Addr              string
	CurrentRouteEpoch func() uint64
}

type Server struct {
	addr              string
	service           cache.Service
	codec             *protocol.Codec
	currentRouteEpoch func() uint64

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
}

func NewServer(cfg ServerConfig, service cache.Service) *Server {
	return &Server{
		addr:              cfg.Addr,
		service:           service,
		codec:             protocol.NewCodec(),
		currentRouteEpoch: cfg.CurrentRouteEpoch,
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
	if stale, current := s.staleRoute(frame.RouteEpoch); stale {
		return errorFrame(frame, protocol.ErrorNoRoute, fmt.Errorf("stale route epoch %d < %d", frame.RouteEpoch, current))
	}
	return s.dispatchFrame(ctx, frame)
}

func (s *Server) dispatchFrame(ctx context.Context, frame protocol.Frame) protocol.Frame {
	switch frame.Op {
	case protocol.OpCacheSet:
		return s.handleSet(ctx, frame)
	case protocol.OpCacheGet:
		return s.handleGet(ctx, frame)
	case protocol.OpCacheDelete:
		return s.handleDelete(ctx, frame)
	case protocol.OpCacheExists:
		return s.handleExists(ctx, frame)
	case protocol.OpCacheTouch:
		return s.handleTouch(ctx, frame)
	case protocol.OpCacheBatchSet:
		return s.handleBatchSet(ctx, frame)
	case protocol.OpCacheBatchGet:
		return s.handleBatchGet(ctx, frame)
	case protocol.OpNodeHeartbeat, protocol.OpControlSnapshot, protocol.OpControlWatch:
		return errorFrame(frame, protocol.ErrorBadFrame, fmt.Errorf("unsupported cache op %d", frame.Op))
	default:
		return errorFrame(frame, protocol.ErrorBadFrame, fmt.Errorf("unsupported cache op %d", frame.Op))
	}
}

func (s *Server) staleRoute(routeEpoch uint64) (bool, uint64) {
	if routeEpoch == 0 || s.currentRouteEpoch == nil {
		return false, 0
	}
	current := s.currentRouteEpoch()
	return current > 0 && routeEpoch < current, current
}

func (s *Server) handleSet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.SetRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	value := frame.Payload
	rec, err := s.service.Set(ctx, keyFromWire(request.Key), value, cache.SetOptions{
		TTL:              ttlFromMillis(request.TTLMillis),
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return recordFrame(frame, rec, true)
}

func (s *Server) handleGet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.GetRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	rec, found, err := s.service.Get(ctx, keyFromWire(request.Key), cache.GetOptions{
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return recordFrame(frame, rec, found)
}

func (s *Server) handleDelete(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.DeleteRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	deleted, err := s.service.Delete(ctx, keyFromWire(request.Key))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return jsonFrame(frame, cachewire.DeleteResponse{Deleted: deleted}, nil)
}

func (s *Server) handleExists(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.ExistsRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	exists, err := s.service.Exists(ctx, keyFromWire(request.Key), getOptionsFromExists(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return jsonFrame(frame, cachewire.ExistsResponse{Exists: exists}, nil)
}

func (s *Server) handleTouch(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.TouchRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	touched, err := s.service.Touch(ctx, keyFromWire(request.Key), touchOptionsFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return jsonFrame(frame, cachewire.TouchResponse{Touched: touched}, nil)
}

func (s *Server) handleBatchSet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.BatchSetRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	items, err := cachewire.UnpackBatchSet(request, frame.Payload)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	records, err := s.service.BatchSet(ctx, batchSetRequests(items))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return jsonFrame(frame, cachewire.BatchSetResponse{Records: recordsFromCache(records)}, nil)
}

func (s *Server) handleBatchGet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	var request cachewire.BatchGetRequest
	if err := json.Unmarshal(frame.Metadata, &request); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchGet(ctx, batchGetRequests(request.Items))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	response, payload, err := cachewire.PackRecords(recordsFromResults(results))
	if err != nil {
		return errorFrame(frame, protocol.ErrorInternal, err)
	}
	return jsonFrame(frame, response, payload)
}

func recordFrame(frame protocol.Frame, rec cache.Record, found bool) protocol.Frame {
	if !found {
		return jsonFrame(frame, cachewire.Record{Found: false}, nil)
	}
	body := recordFromCache(rec, true)
	return jsonFrame(frame, body, rec.Value)
}
