package tcp

import (
	"context"
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
	ReplicaTargets    func(cachewire.Key) []string
}

type Server struct {
	addr              string
	service           cache.Service
	codec             *protocol.Codec
	currentRouteEpoch func() uint64
	replicaTargets    func(cachewire.Key) []string
	replication       *replicationDispatcher
	fences            *rangeFenceSet

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
}

type frameHandler func(context.Context, protocol.Frame) protocol.Frame

func NewServer(cfg ServerConfig, service cache.Service) *Server {
	return &Server{
		addr:              cfg.Addr,
		service:           service,
		codec:             protocol.NewCodec(),
		currentRouteEpoch: cfg.CurrentRouteEpoch,
		replicaTargets:    cfg.ReplicaTargets,
		replication:       newReplicationDispatcher(NewClient(), defaultReplicationTimeout, defaultReplicationQueueSize),
		fences:            newRangeFenceSet(),
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

func (s *Server) ReplicationStats() ReplicationStats {
	if s.replication == nil {
		return ReplicationStats{}
	}
	return s.replication.Stats()
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
		if err := s.replication.Stop(ctx); err != nil {
			return fmt.Errorf("stop cache tcp replication: %w", err)
		}
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
	if fenced, err := s.fencedMutation(frame); err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	} else if fenced {
		return errorFrame(frame, protocol.ErrorNoRoute, fmt.Errorf("fenced migration range for op %d", frame.Op))
	}
	return s.dispatchFrame(ctx, frame)
}

func (s *Server) dispatchFrame(ctx context.Context, frame protocol.Frame) protocol.Frame {
	handler, ok := s.cacheHandlers()[frame.Op]
	if !ok {
		return errorFrame(frame, protocol.ErrorBadFrame, fmt.Errorf("unsupported cache op %d", frame.Op))
	}
	return handler(ctx, frame)
}

func (s *Server) cacheHandlers() map[protocol.Op]frameHandler {
	return map[protocol.Op]frameHandler{
		protocol.OpCacheSet:            s.handleSet,
		protocol.OpCacheGet:            s.handleGet,
		protocol.OpCacheDelete:         s.handleDelete,
		protocol.OpCacheExists:         s.handleExists,
		protocol.OpCacheTouch:          s.handleTouch,
		protocol.OpCacheAdjust:         s.handleAdjust,
		protocol.OpCachePrimitive:      s.handlePrimitive,
		protocol.OpCacheBatchSet:       s.handleBatchSet,
		protocol.OpCacheBatchGet:       s.handleBatchGet,
		protocol.OpCacheBatchPrimitive: s.handleBatchPrimitive,
		protocol.OpCacheBatchDelete:    s.handleBatchDelete,
		protocol.OpCacheBatchExists:    s.handleBatchExists,
		protocol.OpCacheBatchTouch:     s.handleBatchTouch,
		protocol.OpNodeHeartbeat:       s.handleUnsupportedFrame,
		protocol.OpNodeExportRange:     s.handleExportRange,
		protocol.OpNodeImportSnapshot:  s.handleImportSnapshot,
		protocol.OpNodeDeleteRange:     s.handleDeleteRange,
		protocol.OpNodeFenceRange:      s.handleFenceRange,
		protocol.OpNodeUnfenceRange:    s.handleUnfenceRange,
		protocol.OpControlSnapshot:     s.handleUnsupportedFrame,
		protocol.OpControlWatch:        s.handleUnsupportedFrame,
	}
}

func (s *Server) handleUnsupportedFrame(_ context.Context, frame protocol.Frame) protocol.Frame {
	return errorFrame(frame, protocol.ErrorBadFrame, fmt.Errorf("unsupported cache op %d", frame.Op))
}

func (s *Server) staleRoute(routeEpoch uint64) (bool, uint64) {
	if routeEpoch == 0 || s.currentRouteEpoch == nil {
		return false, 0
	}
	current := s.currentRouteEpoch()
	return current > 0 && routeEpoch < current, current
}

func (s *Server) handleSet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeSetRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	value := frame.Payload
	result, err := s.service.Set(ctx, keyFromWire(request.Key), value, cache.SetOptions{
		TTL:              ttlFromMillis(request.TTLMillis),
		NamespaceVersion: request.NamespaceVersion,
		SpaceVersion:     request.SpaceVersion,
		ExpectedVersion:  request.ExpectedVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	if result.Found {
		request.Value = value
		s.replicateSet(request)
	}
	return recordFrame(frame, result.Record, result.Found)
}

func (s *Server) handleGet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeGetRequest(frame.Metadata)
	if err != nil {
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
	request, err := cachewire.DecodeDeleteRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	deleted, applied, err := s.service.Delete(ctx, keyFromWire(request.Key), cache.DeleteOptions{
		ExpectedVersion: request.ExpectedVersion,
	})
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	if deleted && applied {
		s.replicateDelete(request)
	}
	return metadataFrame(frame, cachewire.EncodeDeleteResponse(cachewire.DeleteResponse{Deleted: deleted && applied}), nil)
}

func (s *Server) handleExists(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeExistsRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	exists, err := s.service.Exists(ctx, keyFromWire(request.Key), getOptionsFromExists(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return metadataFrame(frame, cachewire.EncodeExistsResponse(cachewire.ExistsResponse{Exists: exists}), nil)
}

func (s *Server) handleTouch(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeTouchRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	touched, err := s.service.Touch(ctx, keyFromWire(request.Key), touchOptionsFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	if touched {
		s.replicateTouch(request)
	}
	return metadataFrame(frame, cachewire.EncodeTouchResponse(cachewire.TouchResponse{Touched: touched}), nil)
}

func (s *Server) handleAdjust(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeAdjustRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	result, err := s.service.Adjust(ctx, keyFromWire(request.Key), adjustOptionsFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	if result.Found {
		s.replicateAdjust(request)
	}
	return recordFrame(frame, result.Record, result.Found)
}
