package control

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type fakeMigrationRangeClient struct {
	fenceErrs   []error
	unfenceErrs []error
	deleteErrs  []error
	deleteCount uint64
	calls       []string
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
	return cachewire.MigrationSnapshot{}, nil
}

func (f *fakeMigrationRangeClient) ImportSnapshot(
	_ context.Context, _ string, _ cachewire.MigrationSnapshot,
) (cachewire.MigrationImportResponse, error) {
	f.calls = append(f.calls, "import")
	return cachewire.MigrationImportResponse{Imported: 0}, nil
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
	err, remaining := popErr(f.fenceErrs)
	f.fenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) popUnfenceErr() (cachewire.MigrationFenceResponse, error) {
	err, remaining := popErr(f.unfenceErrs)
	f.unfenceErrs = remaining
	return cachewire.MigrationFenceResponse{}, err
}

func (f *fakeMigrationRangeClient) nextDeleteErr() error {
	err, remaining := popErr(f.deleteErrs)
	f.deleteErrs = remaining
	return err
}

func popErr(errs []error) (error, []error) {
	if len(errs) == 0 {
		return nil, nil
	}
	return errs[0], errs[1:]
}

func TestMigrateRangeCleanupUnfencesSource(t *testing.T) {
	t.Parallel()

	client := &fakeMigrationRangeClient{}
	task := controlapi.MigrationTaskBody{
		PlanID:          1,
		TaskID:          1,
		SourceAddr:      "source",
		TargetAddr:      "target",
		CutoverAtUnix:   123,
		ImportedEntries: 11,
	}
	imported, deleted, err := migrateRange(t.Context(), nil, client, MigrationConfig{
		TaskTimeout: time.Second,
	}, task)
	if err != nil {
		t.Fatalf("migrate cleanup range: %v", err)
	}
	if imported != 11 {
		t.Fatalf("imported=%d, want 11", imported)
	}
	if deleted != 3 {
		t.Fatalf("deleted=%d, want 3", deleted)
	}
	if !slices.Equal(client.calls, []string{"delete", "unfence"}) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{"delete", "unfence"})
	}
}

func TestMigrateRangeCleanupPropagatesUnfenceError(t *testing.T) {
	t.Parallel()

	wantUnfenceErr := errors.New("temporary network")
	client := &fakeMigrationRangeClient{
		unfenceErrs: []error{wantUnfenceErr},
	}
	task := controlapi.MigrationTaskBody{
		PlanID:          1,
		TaskID:          1,
		SourceAddr:      "source",
		TargetAddr:      "target",
		CutoverAtUnix:   123,
		ImportedEntries: 7,
	}
	_, _, err := migrateRange(t.Context(), nil, client, MigrationConfig{
		TaskTimeout: time.Second,
	}, task)
	if err == nil {
		t.Fatal("expected unfence error")
	}
	if !errors.Is(err, wantUnfenceErr) {
		t.Fatalf("err = %v, want unfence error", err)
	}
	if !slices.Equal(client.calls, []string{"delete", "unfence"}) {
		t.Fatalf("calls = %#v, want %#v", client.calls, []string{"delete", "unfence"})
	}
}
