package tcp

import (
	"slices"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func (s *Server) fencedMutation(frame protocol.Frame) (bool, error) {
	if s.fences.empty() || !cacheOpCanMutate(frame.Op) {
		return false, nil
	}
	keys, err := mutationKeys(frame)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(keys, s.fences.containsKey), nil
}

var mutatingCacheOps = []protocol.Op{
	protocol.OpCacheSet,
	protocol.OpCacheDelete,
	protocol.OpCacheTouch,
	protocol.OpCacheAdjust,
	protocol.OpCachePrimitive,
	protocol.OpCacheBatchSet,
	protocol.OpCacheBatchDelete,
	protocol.OpCacheBatchTouch,
	protocol.OpCacheBatchPrimitive,
}

func cacheOpCanMutate(op protocol.Op) bool {
	return slices.Contains(mutatingCacheOps, op)
}

var mutationKeyDecoders = map[protocol.Op]func(protocol.Frame) ([]cachewire.Key, error){
	protocol.OpCacheGet:            nil,
	protocol.OpCacheSet:            keyFromSetFrame,
	protocol.OpCacheDelete:         keyFromDeleteFrame,
	protocol.OpCacheBatchGet:       nil,
	protocol.OpNodeHeartbeat:       nil,
	protocol.OpControlSnapshot:     nil,
	protocol.OpControlWatch:        nil,
	protocol.OpCacheExists:         nil,
	protocol.OpCacheTouch:          keyFromTouchFrame,
	protocol.OpCacheAdjust:         keyFromAdjustFrame,
	protocol.OpCachePrimitive:      keysFromPrimitiveFrame,
	protocol.OpCacheBatchSet:       keysFromBatchSetFrame,
	protocol.OpCacheBatchDelete:    keysFromBatchDeleteFrame,
	protocol.OpCacheBatchExists:    nil,
	protocol.OpCacheBatchTouch:     keysFromBatchTouchFrame,
	protocol.OpCacheBatchPrimitive: keysFromBatchPrimitiveFrame,
	protocol.OpNodeExportRange:     nil,
	protocol.OpNodeImportSnapshot:  nil,
	protocol.OpNodeDeleteRange:     nil,
	protocol.OpNodeFenceRange:      nil,
	protocol.OpNodeUnfenceRange:    nil,
}

func mutationKeys(frame protocol.Frame) ([]cachewire.Key, error) {
	decode, ok := mutationKeyDecoders[frame.Op]
	if !ok || decode == nil {
		return nil, nil
	}
	return decode(frame)
}

func keyFromSetFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeSetRequest(frame.Metadata)
	return singleMutationKey(request.Key, err)
}

func keyFromDeleteFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeDeleteRequest(frame.Metadata)
	return singleMutationKey(request.Key, err)
}

func keyFromTouchFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeTouchRequest(frame.Metadata)
	return singleMutationKey(request.Key, err)
}

func keyFromAdjustFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeAdjustRequest(frame.Metadata)
	return singleMutationKey(request.Key, err)
}

func singleMutationKey(key cachewire.Key, err error) ([]cachewire.Key, error) {
	if err != nil {
		return nil, err
	}
	return []cachewire.Key{key}, nil
}
