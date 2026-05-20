package tcp

import (
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func keysFromPrimitiveFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodePrimitiveRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return nil, err
	}
	if !primitiveWireKindMutates(request.Kind) {
		return nil, nil
	}
	return []cachewire.Key{request.Key}, nil
}

func keysFromBatchSetFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeBatchSetRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return nil, err
	}
	keys := make([]cachewire.Key, 0, len(request.Items))
	for index := range request.Items {
		keys = append(keys, request.Items[index].Key)
	}
	return keys, nil
}

func keysFromBatchDeleteFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeBatchDeleteRequest(frame.Metadata)
	if err != nil {
		return nil, err
	}
	keys := make([]cachewire.Key, 0, len(request.Items))
	for index := range request.Items {
		keys = append(keys, request.Items[index].Key)
	}
	return keys, nil
}

func keysFromBatchTouchFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeBatchTouchRequest(frame.Metadata)
	if err != nil {
		return nil, err
	}
	keys := make([]cachewire.Key, 0, len(request.Items))
	for index := range request.Items {
		keys = append(keys, request.Items[index].Key)
	}
	return keys, nil
}

func keysFromBatchPrimitiveFrame(frame protocol.Frame) ([]cachewire.Key, error) {
	request, err := cachewire.DecodeBatchPrimitiveRequest(frame.Metadata, frame.Payload)
	if err != nil {
		return nil, err
	}
	keys := make([]cachewire.Key, 0, len(request.Items))
	for index := range request.Items {
		item := request.Items[index]
		if primitiveWireKindMutates(item.Kind) {
			keys = append(keys, item.Key)
		}
	}
	return keys, nil
}

func primitiveWireKindMutates(kind cachewire.PrimitiveKind) bool {
	switch kind {
	case cachewire.PrimitiveCounterAdjust,
		cachewire.PrimitiveMapSet,
		cachewire.PrimitiveMapDelete,
		cachewire.PrimitiveSetAdd,
		cachewire.PrimitiveSetRemove,
		cachewire.PrimitiveScoredSetPut,
		cachewire.PrimitiveScoredSetRemove,
		cachewire.PrimitiveListPushFront,
		cachewire.PrimitiveListPushBack,
		cachewire.PrimitiveListPopFront,
		cachewire.PrimitiveListPopBack:
		return true
	}
	return false
}
