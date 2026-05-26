package cache

import (
	"context"
	"fmt"

	"github.com/samber/oops"
)

var ErrNestedTransactionUnsupported = oops.Code("nested_transaction_unsupported").
	In("cache").
	New("cache: nested transaction unsupported")

func (s *EngineService) Transaction(ctx context.Context, fn TransactionFunc) error {
	if fn == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx, transactionService{parent: s})
}

type transactionService struct {
	parent *EngineService
}

func (s transactionService) Set(ctx context.Context, key Key, value []byte, opts SetOptions) (SetResult, error) {
	return s.parent.setLocked(ctx, key, value, opts)
}

func (s transactionService) Get(ctx context.Context, key Key, opts GetOptions) (Record, bool, error) {
	return s.parent.getLocked(ctx, key, opts)
}

func (s transactionService) Delete(ctx context.Context, key Key, opts DeleteOptions) (bool, bool, error) {
	return s.parent.deleteLocked(ctx, key, opts)
}

func (s transactionService) Exists(ctx context.Context, key Key, opts GetOptions) (bool, error) {
	return s.parent.existsLocked(ctx, key, opts)
}

func (s transactionService) Touch(ctx context.Context, key Key, opts TouchOptions) (bool, error) {
	return s.parent.touchLocked(ctx, key, opts)
}

func (s transactionService) Adjust(ctx context.Context, key Key, opts AdjustOptions) (SetResult, error) {
	return s.parent.adjustLocked(ctx, key, opts)
}

func (s transactionService) Primitive(ctx context.Context, request PrimitiveRequest) (PrimitiveResult, error) {
	return s.parent.primitiveLocked(ctx, request)
}

func (s transactionService) BatchPrimitive(
	ctx context.Context,
	requests []PrimitiveRequest,
) ([]PrimitiveResult, error) {
	return s.parent.batchPrimitiveLocked(ctx, requests)
}

func (s transactionService) BatchSet(ctx context.Context, requests []SetRequest) ([]SetResult, error) {
	return s.parent.batchSetLocked(ctx, requests)
}

func (s transactionService) BatchGet(ctx context.Context, requests []GetRequest) ([]GetResult, error) {
	return s.parent.batchGetLocked(ctx, requests)
}

func (s transactionService) BatchDelete(ctx context.Context, requests []DeleteRequest) ([]DeleteResult, error) {
	return s.parent.batchDeleteLocked(ctx, requests)
}

func (s transactionService) BatchExists(ctx context.Context, requests []GetRequest) ([]ExistsResult, error) {
	return s.parent.batchExistsLocked(ctx, requests)
}

func (s transactionService) BatchTouch(ctx context.Context, requests []TouchRequest) ([]TouchResult, error) {
	return s.parent.batchTouchLocked(ctx, requests)
}

func (s transactionService) Transaction(context.Context, TransactionFunc) error {
	return fmt.Errorf("%w", ErrNestedTransactionUnsupported)
}

func (s transactionService) Stats(ctx context.Context) (Stats, error) {
	return s.parent.statsLocked(ctx)
}

func (s transactionService) Evict(ctx context.Context, request EvictRequest) (EvictResult, error) {
	return s.parent.evictLocked(ctx, request)
}

func (s transactionService) Export(ctx context.Context, opts RangeOptions) (Snapshot, error) {
	return s.parent.exportLocked(ctx, opts)
}

func (s transactionService) Import(ctx context.Context, snapshot Snapshot) (ImportResult, error) {
	return s.parent.importLocked(ctx, snapshot)
}

func (s transactionService) DeleteRange(ctx context.Context, opts RangeOptions) (DeleteRangeResult, error) {
	return s.parent.deleteRangeLocked(ctx, opts)
}
