package tcp

import (
	"context"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (s *Server) handleExportRange(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeMigrationRangeRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	snapshot, err := s.service.Export(ctx, rangeFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	metadata, payload, err := cachewire.EncodeMigrationSnapshot(snapshotToWire(snapshot))
	if err != nil {
		return errorFrame(frame, protocol.ErrorInternal, err)
	}
	return metadataFrame(frame, metadata, payload)
}

func (s *Server) handleImportSnapshot(ctx context.Context, frame protocol.Frame) protocol.Frame {
	snapshot, err := cachewire.DecodeMigrationSnapshot(frame.Metadata, frame.Payload)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	result, err := s.service.Import(ctx, snapshotFromWire(snapshot))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return metadataFrame(frame, cachewire.EncodeMigrationImportResponse(cachewire.MigrationImportResponse{
		Imported: result.Imported,
	}), nil)
}

func (s *Server) handleDeleteRange(ctx context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeMigrationRangeRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	result, err := s.service.DeleteRange(ctx, rangeFromWire(request))
	if err != nil {
		return cacheErrorFrame(frame, err)
	}
	return metadataFrame(frame, cachewire.EncodeMigrationDeleteRangeResponse(cachewire.MigrationDeleteRangeResponse{
		Deleted: result.Deleted,
	}), nil)
}

func (s *Server) handleFenceRange(_ context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeMigrationRangeRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	s.fences.add(rangeFenceFromWire(request))
	return metadataFrame(frame, cachewire.EncodeMigrationFenceResponse(cachewire.MigrationFenceResponse{
		Applied: true,
	}), nil)
}

func (s *Server) handleUnfenceRange(_ context.Context, frame protocol.Frame) protocol.Frame {
	request, err := cachewire.DecodeMigrationRangeRequest(frame.Metadata)
	if err != nil {
		return errorFrame(frame, protocol.ErrorBadFrame, err)
	}
	s.fences.remove(rangeFenceFromWire(request))
	return metadataFrame(frame, cachewire.EncodeMigrationFenceResponse(cachewire.MigrationFenceResponse{
		Applied: true,
	}), nil)
}
