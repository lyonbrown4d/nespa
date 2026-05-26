package engine

import (
	"context"
	"strconv"
)

type primitiveEstimateHandler func(*shardWorker, shardCommand) shardResult

var mutatingPrimitiveEstimateHandlers = map[uint8]primitiveEstimateHandler{
	uint8(PrimitiveCounterAdjust):   (*shardWorker).estimateCounterPrimitive,
	uint8(PrimitiveMapSet):          (*shardWorker).estimateMapPrimitive,
	uint8(PrimitiveMapDelete):       (*shardWorker).estimateMapPrimitive,
	uint8(PrimitiveSetAdd):          (*shardWorker).estimateSetPrimitive,
	uint8(PrimitiveSetRemove):       (*shardWorker).estimateSetPrimitive,
	uint8(PrimitiveScoredSetPut):    (*shardWorker).estimateScoredSetPrimitive,
	uint8(PrimitiveScoredSetRemove): (*shardWorker).estimateScoredSetPrimitive,
	uint8(PrimitiveListPushFront):   (*shardWorker).estimateListPrimitive,
	uint8(PrimitiveListPushBack):    (*shardWorker).estimateListPrimitive,
	uint8(PrimitiveListPopFront):    (*shardWorker).estimateListPrimitive,
	uint8(PrimitiveListPopBack):     (*shardWorker).estimateListPrimitive,
	uint8(PrimitiveBitmapSetBit):    (*shardWorker).estimateBitmapPrimitive,
	uint8(PrimitiveHLLAdd):          (*shardWorker).estimateHLLPrimitive,
	uint8(PrimitiveHLLMerge):        (*shardWorker).estimateHLLPrimitive,
	uint8(PrimitiveGeoAdd):          (*shardWorker).estimateGeoPrimitive,
}

func (e *MemoryEngine) EstimatePrimitive(
	ctx context.Context,
	request PrimitiveRequest,
) (PrimitiveEstimate, error) {
	if err := validatePrimitiveRequest(request); err != nil {
		return PrimitiveEstimate{}, err
	}

	request.Value = append([]byte(nil), request.Value...)
	result, err := e.execute(ctx, shardCommand{
		kind:      commandPrimitiveEstimate,
		physical:  physicalKey(request.Key),
		key:       request.Key,
		primitive: request,
		now:       e.now(),
		reply:     make(chan shardResult, 1),
	})
	if err != nil {
		return PrimitiveEstimate{}, err
	}
	return result.estimate, result.err
}

func (s *shardWorker) applyPrimitiveEstimate(cmd shardCommand) shardResult {
	if !cmd.primitive.Kind.Mutates() {
		return shardResult{estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	if handler, ok := mutatingPrimitiveEstimateHandlers[uint8(cmd.primitive.Kind)]; ok {
		return handler(s, cmd)
	}
	return shardResult{err: primitiveValidationError(cmd.primitive.Kind, "unknown kind")}
}

func (s *shardWorker) estimateCounterPrimitive(cmd shardCommand) shardResult {
	next := cmd
	next.adjust = AdjustOptions{
		Delta:            cmd.primitive.Delta,
		InitialValue:     cmd.primitive.InitialValue,
		TTL:              cmd.primitive.Options.TTL,
		NamespaceVersion: cmd.primitive.Options.NamespaceVersion,
		SpaceVersion:     cmd.primitive.Options.SpaceVersion,
		ExpectedVersion:  cmd.primitive.Options.ExpectedVersion,
	}
	adjustment, ok, err := s.prepareCounterAdjustment(next)
	if err != nil || !ok {
		return shardResult{err: err, estimate: PrimitiveEstimate{Key: cmd.key}}
	}
	value := []byte(strconv.FormatInt(adjustment.next, 10))
	return shardResult{estimate: estimateWriteCost(cmd, adjustment.existing, adjustment.exists, value)}
}

func estimateWriteCost(cmd shardCommand, ent *entry, exists bool, value []byte) WriteEstimate {
	oldCost := uint64(0)
	if exists && ent != nil {
		oldCost = ent.costBytes
	}
	return WriteEstimate{
		Key:          cmd.key,
		Applied:      true,
		OldCostBytes: oldCost,
		NewCostBytes: costOf(cmd.key, value),
	}
}
