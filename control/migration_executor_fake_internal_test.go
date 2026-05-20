package control

import (
	"context"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
)

type fakeMigrationRangeClient struct {
	fenceErrs       []error
	unfenceErrs     []error
	deleteErrs      []error
	exportErrs      []error
	exportSnapshots []cachewire.MigrationSnapshot
	importErrs      []error
	importResponses []cachewire.MigrationImportResponse
	deleteCount     uint64
	calls           []string
}

func (f *fakeMigrationRangeClient) FenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	f.calls = append(f.calls, "fence")
	return f.popFenceErr()
}

func (f *fakeMigrationRangeClient) UnfenceRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationFenceResponse, error) {
	f.calls = append(f.calls, "unfence")
	return f.popUnfenceErr()
}

func (f *fakeMigrationRangeClient) ExportRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationSnapshot, error) {
	f.calls = append(f.calls, "export")
	snapshot, remaining := popSnapshot(f.exportSnapshots)
	f.exportSnapshots = remaining
	remainingErr, err := popErr(f.exportErrs)
	f.exportErrs = remainingErr
	return snapshot, err
}

func (f *fakeMigrationRangeClient) ImportSnapshot(
	_ context.Context, _ string, _ cachewire.MigrationSnapshot,
) (cachewire.MigrationImportResponse, error) {
	f.calls = append(f.calls, "import")
	response, remaining := popImportResponse(f.importResponses)
	f.importResponses = remaining
	remainingErr, err := popErr(f.importErrs)
	f.importErrs = remainingErr
	if err != nil {
		return response, err
	}
	return response, nil
}

func (f *fakeMigrationRangeClient) DeleteRange(
	_ context.Context, _ string, _ cachewire.MigrationRangeRequest,
) (cachewire.MigrationDeleteRangeResponse, error) {
	f.calls = append(f.calls, "delete")
	f.deleteCount++
	err := f.nextDeleteErr()
	if err != nil {
		return cachewire.MigrationDeleteRangeResponse{}, err
	}
	return cachewire.MigrationDeleteRangeResponse{Deleted: 3}, nil
}

func (f *fakeMigrationRangeClient) popFenceErr() (cachewire.MigrationFenceResponse, error) {
	remaining, err := popErr(f.fenceErrs)
	f.fenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) popUnfenceErr() (cachewire.MigrationFenceResponse, error) {
	remaining, err := popErr(f.unfenceErrs)
	f.unfenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) nextDeleteErr() error {
	remaining, err := popErr(f.deleteErrs)
	f.deleteErrs = remaining
	return err
}

func popErr(errs []error) ([]error, error) {
	if len(errs) == 0 {
		return nil, nil
	}
	return errs[1:], errs[0]
}

func popSnapshot(snapshots []cachewire.MigrationSnapshot) (cachewire.MigrationSnapshot, []cachewire.MigrationSnapshot) {
	if len(snapshots) == 0 {
		return cachewire.MigrationSnapshot{}, nil
	}
	return snapshots[0], snapshots[1:]
}

func popImportResponse(
	responses []cachewire.MigrationImportResponse,
) (cachewire.MigrationImportResponse, []cachewire.MigrationImportResponse) {
	if len(responses) == 0 {
		return cachewire.MigrationImportResponse{Imported: 0}, responses
	}
	return responses[0], responses[1:]
}

func newMigrationTestService(state *ControlState) *ServiceRuntime {
	return newMigrationTestServiceWithClock(state, nil)
}

func newMigrationTestServiceWithClock(state *ControlState, now func() time.Time) *ServiceRuntime {
	return &ServiceRuntime{
		state: state,
		fsm:   NewControlFSM(state),
		now:   now,
	}
}
