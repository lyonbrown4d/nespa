package tcp

import (
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/nespa/cachewire"
)

type replicationCommandDecoder func(replicationOutboxEntry) (replicationCommand, error)

var replicationCommandDecoders = map[replicationCommandKind]replicationCommandDecoder{
	replicationCommandSet:            newReplicationCommandFromSet,
	replicationCommandDelete:         newReplicationCommandFromDelete,
	replicationCommandTouch:          newReplicationCommandFromTouch,
	replicationCommandAdjust:         newReplicationCommandFromAdjust,
	replicationCommandPrimitive:      newReplicationCommandFromPrimitive,
	replicationCommandBatchSet:       newReplicationCommandFromBatchSet,
	replicationCommandBatchDelete:    newReplicationCommandFromBatchDelete,
	replicationCommandBatchTouch:     newReplicationCommandFromBatchTouch,
	replicationCommandBatchPrimitive: newReplicationCommandFromBatchPrimitive,
}

func replicationCommandFromOutboxEntry(entry replicationOutboxEntry) (replicationCommand, error) {
	decoder, ok := replicationCommandDecoders[entry.Kind]
	if !ok {
		return replicationCommand{}, fmt.Errorf("decode replication outbox: unknown command kind %d", entry.Kind)
	}
	return decoder(entry)
}

func newReplicationCommandFromSet(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeSetRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox set request: %w", err)
	}
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	request.Value = append([]byte(nil), entry.Payload...)
	return replicationCommand{
		kind: replicationCommandSet,
		set:  request,
	}, nil
}

func newReplicationCommandFromDelete(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeDeleteRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox delete request: %w", err)
	}
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return replicationCommand{
		kind:   replicationCommandDelete,
		delete: request,
	}, nil
}

func newReplicationCommandFromTouch(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeTouchRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox touch request: %w", err)
	}
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return replicationCommand{
		kind:  replicationCommandTouch,
		touch: request,
	}, nil
}

func newReplicationCommandFromAdjust(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeAdjustRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox adjust request: %w", err)
	}
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return replicationCommand{
		kind:   replicationCommandAdjust,
		adjust: request,
	}, nil
}

func newReplicationCommandFromPrimitive(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodePrimitiveRequest(entry.Metadata, entry.Payload)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox primitive request: %w", err)
	}
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return replicationCommand{
		kind:      replicationCommandPrimitive,
		primitive: request,
	}, nil
}

func newReplicationCommandFromBatchSet(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeBatchSetRequest(entry.Metadata, entry.Payload)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox batch set request: %w", err)
	}
	return replicationCommand{
		kind:     replicationCommandBatchSet,
		batchSet: cachewire.BatchSetRequest{Items: cloneReplicaSetRequests(request.Items)},
	}, nil
}

func newReplicationCommandFromBatchDelete(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeBatchDeleteRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox batch delete request: %w", err)
	}
	return replicationCommand{
		kind:        replicationCommandBatchDelete,
		batchDelete: cachewire.BatchDeleteRequest{Items: cloneReplicaDeleteRequests(request.Items)},
	}, nil
}

func newReplicationCommandFromBatchTouch(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeBatchTouchRequest(entry.Metadata)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox batch touch request: %w", err)
	}
	return replicationCommand{
		kind:       replicationCommandBatchTouch,
		batchTouch: cachewire.BatchTouchRequest{Items: cloneReplicaTouchRequests(request.Items)},
	}, nil
}

func newReplicationCommandFromBatchPrimitive(entry replicationOutboxEntry) (replicationCommand, error) {
	request, err := cachewire.DecodeBatchPrimitiveRequest(entry.Metadata, entry.Payload)
	if err != nil {
		return replicationCommand{}, fmt.Errorf("decode replication outbox batch primitive request: %w", err)
	}
	return replicationCommand{
		kind:           replicationCommandBatchPrimitive,
		batchPrimitive: cachewire.BatchPrimitiveRequest{Items: cloneReplicaPrimitiveRequests(request.Items)},
	}, nil
}

func cloneReplicaSetRequests(items []cachewire.SetRequest) []cachewire.SetRequest {
	cloned := collectionlist.NewListWithCapacity[cachewire.SetRequest](len(items))
	for index := range items {
		cloned.Add(replicaSetRequest(items[index]))
	}
	return cloned.Values()
}

func cloneReplicaDeleteRequests(items []cachewire.DeleteRequest) []cachewire.DeleteRequest {
	cloned := collectionlist.NewListWithCapacity[cachewire.DeleteRequest](len(items))
	for index := range items {
		cloned.Add(replicaDeleteRequest(items[index]))
	}
	return cloned.Values()
}

func cloneReplicaTouchRequests(items []cachewire.TouchRequest) []cachewire.TouchRequest {
	cloned := collectionlist.NewListWithCapacity[cachewire.TouchRequest](len(items))
	for index := range items {
		cloned.Add(replicaTouchRequest(items[index]))
	}
	return cloned.Values()
}

func cloneReplicaPrimitiveRequests(items []cachewire.PrimitiveRequest) []cachewire.PrimitiveRequest {
	cloned := collectionlist.NewListWithCapacity[cachewire.PrimitiveRequest](len(items))
	for index := range items {
		cloned.Add(replicaPrimitiveRequest(items[index]))
	}
	return cloned.Values()
}
