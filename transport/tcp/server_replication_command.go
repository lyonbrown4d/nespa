package tcp

import (
	"context"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/samber/oops"
)

type replicationCommandKind uint8

const (
	replicationCommandSet replicationCommandKind = iota + 1
	replicationCommandDelete
	replicationCommandTouch
	replicationCommandAdjust
	replicationCommandPrimitive
	replicationCommandBatchSet
	replicationCommandBatchDelete
	replicationCommandBatchTouch
	replicationCommandBatchPrimitive
)

type replicationCommandSender func(replicationCommand, context.Context, *Client, string) error

var replicationCommandSenders = map[replicationCommandKind]replicationCommandSender{
	replicationCommandSet:            replicationCommand.sendSet,
	replicationCommandDelete:         replicationCommand.sendDelete,
	replicationCommandTouch:          replicationCommand.sendTouch,
	replicationCommandAdjust:         replicationCommand.sendAdjust,
	replicationCommandPrimitive:      replicationCommand.sendPrimitive,
	replicationCommandBatchSet:       replicationCommand.sendBatchSet,
	replicationCommandBatchDelete:    replicationCommand.sendBatchDelete,
	replicationCommandBatchTouch:     replicationCommand.sendBatchTouch,
	replicationCommandBatchPrimitive: replicationCommand.sendBatchPrimitive,
}

type replicationCommand struct {
	kind           replicationCommandKind
	set            cachewire.SetRequest
	delete         cachewire.DeleteRequest
	touch          cachewire.TouchRequest
	adjust         cachewire.AdjustRequest
	primitive      cachewire.PrimitiveRequest
	batchSet       cachewire.BatchSetRequest
	batchDelete    cachewire.BatchDeleteRequest
	batchTouch     cachewire.BatchTouchRequest
	batchPrimitive cachewire.BatchPrimitiveRequest
}

func newSetReplicationCommand(request cachewire.SetRequest) replicationCommand {
	return replicationCommand{
		kind: replicationCommandSet,
		set:  replicaSetRequest(request),
	}
}

func newDeleteReplicationCommand(request cachewire.DeleteRequest) replicationCommand {
	return replicationCommand{
		kind:   replicationCommandDelete,
		delete: replicaDeleteRequest(request),
	}
}

func newTouchReplicationCommand(request cachewire.TouchRequest) replicationCommand {
	return replicationCommand{
		kind:  replicationCommandTouch,
		touch: replicaTouchRequest(request),
	}
}

func newAdjustReplicationCommand(request cachewire.AdjustRequest) replicationCommand {
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return replicationCommand{
		kind:   replicationCommandAdjust,
		adjust: request,
	}
}

func newPrimitiveReplicationCommand(request cachewire.PrimitiveRequest) replicationCommand {
	return replicationCommand{
		kind:      replicationCommandPrimitive,
		primitive: replicaPrimitiveRequest(request),
	}
}

func newBatchSetReplicationCommand(request cachewire.BatchSetRequest) replicationCommand {
	return replicationCommand{
		kind:     replicationCommandBatchSet,
		batchSet: cachewire.BatchSetRequest{Items: cloneReplicaSetRequests(request.Items)},
	}
}

func newBatchDeleteReplicationCommand(request cachewire.BatchDeleteRequest) replicationCommand {
	return replicationCommand{
		kind:        replicationCommandBatchDelete,
		batchDelete: cachewire.BatchDeleteRequest{Items: cloneReplicaDeleteRequests(request.Items)},
	}
}

func newBatchTouchReplicationCommand(request cachewire.BatchTouchRequest) replicationCommand {
	return replicationCommand{
		kind:       replicationCommandBatchTouch,
		batchTouch: cachewire.BatchTouchRequest{Items: cloneReplicaTouchRequests(request.Items)},
	}
}

func newBatchPrimitiveReplicationCommand(request cachewire.BatchPrimitiveRequest) replicationCommand {
	return replicationCommand{
		kind:           replicationCommandBatchPrimitive,
		batchPrimitive: cachewire.BatchPrimitiveRequest{Items: cloneReplicaPrimitiveRequests(request.Items)},
	}
}

func (c replicationCommand) valid() bool {
	return c.kind != 0
}

func (c replicationCommand) send(ctx context.Context, client *Client, target string) error {
	sender, ok := replicationCommandSenders[c.kind]
	if !ok {
		return oops.Code("unknown_replication_command").
			In("transport.tcp").
			With("kind", c.kind).
			Errorf("cache tcp: unknown replication command %d", c.kind)
	}
	return sender(c, ctx, client, target)
}

func (c replicationCommand) sendSet(ctx context.Context, client *Client, target string) error {
	if _, err := client.Set(ctx, target, c.set); err != nil {
		return fmt.Errorf("replicate set: %w", err)
	}
	return nil
}

func (c replicationCommand) sendDelete(ctx context.Context, client *Client, target string) error {
	if _, err := client.Delete(ctx, target, c.delete); err != nil {
		return fmt.Errorf("replicate delete: %w", err)
	}
	return nil
}

func (c replicationCommand) sendTouch(ctx context.Context, client *Client, target string) error {
	if _, err := client.Touch(ctx, target, c.touch); err != nil {
		return fmt.Errorf("replicate touch: %w", err)
	}
	return nil
}

func (c replicationCommand) sendAdjust(ctx context.Context, client *Client, target string) error {
	if _, err := client.Adjust(ctx, target, c.adjust); err != nil {
		return fmt.Errorf("replicate adjust: %w", err)
	}
	return nil
}

func (c replicationCommand) sendPrimitive(ctx context.Context, client *Client, target string) error {
	if _, err := client.Primitive(ctx, target, c.primitive); err != nil {
		return fmt.Errorf("replicate primitive: %w", err)
	}
	return nil
}

func (c replicationCommand) sendBatchSet(ctx context.Context, client *Client, target string) error {
	if _, err := client.BatchSet(ctx, target, c.batchSet); err != nil {
		return fmt.Errorf("replicate batch set: %w", err)
	}
	return nil
}

func (c replicationCommand) sendBatchDelete(ctx context.Context, client *Client, target string) error {
	if _, err := client.BatchDelete(ctx, target, c.batchDelete); err != nil {
		return fmt.Errorf("replicate batch delete: %w", err)
	}
	return nil
}

func (c replicationCommand) sendBatchTouch(ctx context.Context, client *Client, target string) error {
	if _, err := client.BatchTouch(ctx, target, c.batchTouch); err != nil {
		return fmt.Errorf("replicate batch touch: %w", err)
	}
	return nil
}

func (c replicationCommand) sendBatchPrimitive(ctx context.Context, client *Client, target string) error {
	if _, err := client.BatchPrimitive(ctx, target, c.batchPrimitive); err != nil {
		return fmt.Errorf("replicate batch primitive: %w", err)
	}
	return nil
}

func replicationCommandFromOutboxEntry(entry replicationOutboxEntry) (replicationCommand, error) {
	switch entry.Kind {
	case replicationCommandSet:
		return newReplicationCommandFromSet(entry)
	case replicationCommandDelete:
		return newReplicationCommandFromDelete(entry)
	case replicationCommandTouch:
		return newReplicationCommandFromTouch(entry)
	case replicationCommandAdjust:
		return newReplicationCommandFromAdjust(entry)
	case replicationCommandPrimitive:
		return newReplicationCommandFromPrimitive(entry)
	case replicationCommandBatchSet:
		return newReplicationCommandFromBatchSet(entry)
	case replicationCommandBatchDelete:
		return newReplicationCommandFromBatchDelete(entry)
	case replicationCommandBatchTouch:
		return newReplicationCommandFromBatchTouch(entry)
	case replicationCommandBatchPrimitive:
		return newReplicationCommandFromBatchPrimitive(entry)
	default:
		return replicationCommand{}, fmt.Errorf("decode replication outbox: unknown command kind %d", entry.Kind)
	}
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
