package tcp

import (
	"fmt"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
	"github.com/samber/oops"
)

type replicationCommandEncoder func(replicationCommand) (replicationCommandFrame, error)

var replicationCommandEncoders = map[replicationCommandKind]replicationCommandEncoder{
	replicationCommandSet:            encodeSetReplicationCommand,
	replicationCommandDelete:         encodeDeleteReplicationCommand,
	replicationCommandTouch:          encodeTouchReplicationCommand,
	replicationCommandAdjust:         encodeAdjustReplicationCommand,
	replicationCommandPrimitive:      encodePrimitiveReplicationCommand,
	replicationCommandBatchSet:       encodeBatchSetReplicationCommand,
	replicationCommandBatchDelete:    encodeBatchDeleteReplicationCommand,
	replicationCommandBatchTouch:     encodeBatchTouchReplicationCommand,
	replicationCommandBatchPrimitive: encodeBatchPrimitiveReplicationCommand,
}

type replicationCommandFrame struct {
	op       protocol.Op
	metadata []byte
	payload  []byte
}

func (c replicationCommand) encodeFrame() (replicationCommandFrame, error) {
	encoder, ok := replicationCommandEncoders[c.kind]
	if !ok {
		return replicationCommandFrame{}, oops.Code("unknown_replication_command").
			In("transport.tcp").
			With("kind", c.kind).
			Errorf("cache tcp: unknown replication command %d", c.kind)
	}
	return encoder(c)
}

func encodeSetReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheSet,
		metadata: cachewire.EncodeSetRequest(c.set),
		payload:  append([]byte(nil), c.set.Value...),
	}, nil
}

func encodeDeleteReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheDelete,
		metadata: cachewire.EncodeDeleteRequest(c.delete),
	}, nil
}

func encodeTouchReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheTouch,
		metadata: cachewire.EncodeTouchRequest(c.touch),
	}, nil
}

func encodeAdjustReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheAdjust,
		metadata: cachewire.EncodeAdjustRequest(c.adjust),
	}, nil
}

func encodePrimitiveReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	metadata, payload, err := cachewire.EncodePrimitiveRequest(c.primitive)
	if err != nil {
		return replicationCommandFrame{}, fmt.Errorf("encode replication primitive command: %w", err)
	}
	return replicationCommandFrame{op: protocol.OpCachePrimitive, metadata: metadata, payload: payload}, nil
}

func encodeBatchSetReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	metadata, payload, err := cachewire.EncodeBatchSetRequest(c.batchSet)
	if err != nil {
		return replicationCommandFrame{}, fmt.Errorf("encode replication batch set command: %w", err)
	}
	return replicationCommandFrame{op: protocol.OpCacheBatchSet, metadata: metadata, payload: payload}, nil
}

func encodeBatchDeleteReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheBatchDelete,
		metadata: cachewire.EncodeBatchDeleteRequest(c.batchDelete),
	}, nil
}

func encodeBatchTouchReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	return replicationCommandFrame{
		op:       protocol.OpCacheBatchTouch,
		metadata: cachewire.EncodeBatchTouchRequest(c.batchTouch),
	}, nil
}

func encodeBatchPrimitiveReplicationCommand(c replicationCommand) (replicationCommandFrame, error) {
	metadata, payload, err := cachewire.EncodeBatchPrimitiveRequest(c.batchPrimitive)
	if err != nil {
		return replicationCommandFrame{}, fmt.Errorf("encode replication batch primitive command: %w", err)
	}
	return replicationCommandFrame{op: protocol.OpCacheBatchPrimitive, metadata: metadata, payload: payload}, nil
}
