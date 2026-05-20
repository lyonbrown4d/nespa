package tcp

import (
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
	for index := range keys {
		if s.fences.containsKey(keys[index]) {
			return true, nil
		}
	}
	return false, nil
}

func cacheOpCanMutate(op protocol.Op) bool {
	switch op {
	case protocol.OpCacheSet,
		protocol.OpCacheDelete,
		protocol.OpCacheTouch,
		protocol.OpCacheAdjust,
		protocol.OpCachePrimitive,
		protocol.OpCacheBatchSet,
		protocol.OpCacheBatchDelete,
		protocol.OpCacheBatchTouch,
		protocol.OpCacheBatchPrimitive:
		return true
	}
	return false
}

func mutationKeys(frame protocol.Frame) ([]cachewire.Key, error) {
	switch frame.Op {
	case protocol.OpCacheSet:
		return keyFromSetFrame(frame)
	case protocol.OpCacheDelete:
		return keyFromDeleteFrame(frame)
	case protocol.OpCacheTouch:
		return keyFromTouchFrame(frame)
	case protocol.OpCacheAdjust:
		return keyFromAdjustFrame(frame)
	case protocol.OpCachePrimitive:
		return keysFromPrimitiveFrame(frame)
	case protocol.OpCacheBatchSet:
		return keysFromBatchSetFrame(frame)
	case protocol.OpCacheBatchDelete:
		return keysFromBatchDeleteFrame(frame)
	case protocol.OpCacheBatchTouch:
		return keysFromBatchTouchFrame(frame)
	case protocol.OpCacheBatchPrimitive:
		return keysFromBatchPrimitiveFrame(frame)
	}
	return nil, nil
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
