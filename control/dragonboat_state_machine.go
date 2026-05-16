package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	dragonstatemachine "github.com/lni/dragonboat/v3/statemachine"
	"github.com/samber/oops"
)

type dragonboatStateMachine struct {
	state *ControlState
	fsm   *ControlFSM
}

type dragonboatApplyEnvelope struct {
	Result ApplyResult           `json:"result"`
	Error  *dragonboatApplyError `json:"error,omitempty"`
}

type dragonboatApplyError struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

func (sm *dragonboatStateMachine) Update(raw []byte) (dragonstatemachine.Result, error) {
	var command Command
	if err := json.Unmarshal(raw, &command); err != nil {
		return dragonstatemachine.Result{}, oops.Code("control_raft_command_decode_failed").
			In("control.raft").
			Wrapf(err, "decode control raft command")
	}

	result, applyErr := sm.fsm.Apply(context.Background(), command)
	envelope := dragonboatApplyEnvelope{Result: result}
	if applyErr != nil {
		envelope.Error = newDragonboatApplyError(applyErr)
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return dragonstatemachine.Result{}, oops.Code("control_raft_result_encode_failed").
			In("control.raft").
			Wrapf(err, "encode control raft result")
	}
	return dragonstatemachine.Result{
		Value: sm.state.Revision(),
		Data:  data,
	}, nil
}

func (sm *dragonboatStateMachine) Lookup(any) (any, error) {
	return sm.state.ExportSnapshot(), nil
}

func (sm *dragonboatStateMachine) SaveSnapshot(
	writer io.Writer,
	_ dragonstatemachine.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return dragonstatemachine.ErrSnapshotStopped
	default:
	}
	if err := json.NewEncoder(writer).Encode(sm.state.ExportSnapshot()); err != nil {
		return oops.Code("control_raft_snapshot_encode_failed").
			In("control.raft").
			Wrapf(err, "encode control raft snapshot")
	}
	return nil
}

func (sm *dragonboatStateMachine) RecoverFromSnapshot(
	reader io.Reader,
	_ []dragonstatemachine.SnapshotFile,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return dragonstatemachine.ErrSnapshotStopped
	default:
	}

	var snapshot Snapshot
	if err := json.NewDecoder(reader).Decode(&snapshot); err != nil {
		return oops.Code("control_raft_snapshot_decode_failed").
			In("control.raft").
			Wrapf(err, "decode control raft snapshot")
	}
	if err := sm.state.RestoreSnapshot(snapshot); err != nil {
		return oops.Code("control_raft_snapshot_restore_failed").
			In("control.raft").
			Wrapf(err, "restore control raft snapshot")
	}
	return nil
}

func (sm *dragonboatStateMachine) Close() error {
	return nil
}

func decodeDragonboatApplyEnvelope(raw []byte) (dragonboatApplyEnvelope, error) {
	var envelope dragonboatApplyEnvelope
	if len(raw) == 0 {
		return envelope, nil
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return dragonboatApplyEnvelope{}, oops.Code("control_raft_result_decode_failed").
			In("control.raft").
			Wrapf(err, "decode control raft result")
	}
	return envelope, nil
}

func newDragonboatApplyError(err error) *dragonboatApplyError {
	code, _ := controlOopsCode(err)
	return &dragonboatApplyError{
		Message: err.Error(),
		Code:    code,
	}
}

func (e *dragonboatApplyError) toError() error {
	if e == nil {
		return nil
	}
	if e.Code != "" {
		return fmt.Errorf("%w", oops.Code(e.Code).In("control").New(e.Message))
	}
	return fmt.Errorf("%w", oops.Code("control_raft_command_failed").In("control").New(e.Message))
}
