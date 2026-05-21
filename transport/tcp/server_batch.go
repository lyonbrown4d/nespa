package tcp

import (
	"context"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (s *Server) handleBatchSet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchSetRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	records, err := s.service.BatchSet(ctx, batchSetRequests(request.Items))
	if err != nil {
		s.replicateBatchSet(request, records)
		return cacheErrorFrame(frame, err)
	}
	s.replicateBatchSet(request, records)
	return metadataFrame(frame, cachewire.EncodeBatchSetResponse(cachewire.BatchSetResponse{
		Records: recordsFromSetResults(records),
	}), nil)
}

func (s *Server) handleBatchGet(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchGetRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchGet(ctx, batchGetRequests(request.Items))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	metadata, payload, err := cachewire.EncodeBatchGetResponse(cachewire.BatchGetResponse{
		Records: recordsFromResults(results),
	})
	if err != nil {
		return errorFrame(frame, protocol.ErrorInternal, err)
	}
	return metadataFrame(frame, metadata, payload)
}

func (s *Server) handleBatchDelete(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchDeleteRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchDelete(ctx, batchDeleteRequests(request.Items))
	if err != nil {
		s.replicateBatchDelete(request, results)
		return cacheErrorFrame(frame, err)
	}
	s.replicateBatchDelete(request, results)
	return metadataFrame(frame, cachewire.EncodeBatchDeleteResponse(cachewire.BatchDeleteResponse{
		Results: deleteResultsFromCache(results),
	}), nil)
}

func (s *Server) handleBatchExists(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchExistsRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchExists(ctx, batchExistsRequests(request.Items))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return metadataFrame(frame, cachewire.EncodeBatchExistsResponse(cachewire.BatchExistsResponse{
		Results: existsResultsFromCache(results),
	}), nil)
}

func (s *Server) handleBatchTouch(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeBatchTouchRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	results, err := s.service.BatchTouch(ctx, batchTouchRequests(request.Items))
	if err != nil {
		s.replicateBatchTouch(request, results)
		return cacheErrorFrame(frame, err)
	}
	s.replicateBatchTouch(request, results)
	return metadataFrame(frame, cachewire.EncodeBatchTouchResponse(cachewire.BatchTouchResponse{
		Results: touchResultsFromCache(results),
	}), nil)
}
