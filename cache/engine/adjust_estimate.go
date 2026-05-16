package engine

import (
	"context"
	"strconv"
)

func (e *MemoryEngine) EstimateAdjust(
	ctx context.Context,
	key Key,
	opts AdjustOptions,
) (WriteEstimate, error) {
	if err := validateKey(key); err != nil {
		return WriteEstimate{}, err
	}

	result, err := e.execute(ctx, shardCommand{
		kind:     commandAdjustEstimate,
		physical: physicalKey(key),
		key:      key,
		adjust:   opts,
		now:      e.now(),
		reply:    make(chan shardResult, 1),
	})
	if err != nil {
		return WriteEstimate{}, err
	}
	return result.estimate, result.err
}

func (s *shardWorker) applyAdjustEstimate(cmd shardCommand) shardResult {
	adjustment, ok, err := s.prepareCounterAdjustment(cmd)
	if err != nil || !ok {
		return shardResult{err: err, estimate: WriteEstimate{Key: cmd.key}}
	}
	value := []byte(strconv.FormatInt(adjustment.next, 10))
	return shardResult{estimate: estimateWriteCost(cmd, adjustment.existing, adjustment.exists, value)}
}
