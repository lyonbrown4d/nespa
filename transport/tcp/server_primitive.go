package tcp

import (
	"context"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (s *Server) handlePrimitive(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodePrimitiveRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	result, err := s.service.Primitive(ctx, primitiveRequestFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	if result.Applied && cache.PrimitiveKind(request.Kind).Mutates() {
		s.replicatePrimitive(ctx, request)
	}
	metadata, payload, err := cachewire.EncodePrimitiveResponse(primitiveResultFromCache(result))
	if err != nil {
		return errorFrame(frame, protocol.ErrorInternal, err)
	}
	return metadataFrame(frame, metadata, payload)
}

func (s *Server) handleBatchPrimitive(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchPrimitiveRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchPrimitive(ctx, primitiveRequestsFromWire(request.Items))
	if err != nil {
		s.replicateBatchPrimitive(ctx, request, results)
		return cacheErrorFrame(frame, err)
	}
	s.replicateBatchPrimitive(ctx, request, results)
	response := cachewire.BatchPrimitiveResponse{Results: primitiveResultsFromCache(results)}
	metadata, payload, err := cachewire.EncodeBatchPrimitiveResponse(response)
	if err != nil {
		return errorFrame(frame, protocol.ErrorInternal, err)
	}
	return metadataFrame(frame, metadata, payload)
}
