package control

import (
	"context"
	"fmt"
	"time"

	"github.com/lyonbrown4d/nespa/controlapi"
)

type CommandType string

const (
	CommandCreateNamespace       CommandType = "create_namespace"
	CommandCreateSpace           CommandType = "create_space"
	CommandCreateEntity          CommandType = "create_entity"
	CommandBumpNamespace         CommandType = "bump_namespace"
	CommandBumpSpace             CommandType = "bump_space"
	CommandRegisterNode          CommandType = "register_node"
	CommandHeartbeat             CommandType = "heartbeat"
	CommandAdvanceNodeLiveness   CommandType = "advance_node_liveness"
	CommandClaimMigrationTask    CommandType = "claim_migration_task"
	CommandCutoverMigrationTask  CommandType = "cutover_migration_task"
	CommandCompleteMigrationTask CommandType = "complete_migration_task"
	CommandFailMigrationTask     CommandType = "fail_migration_task"
)

type Command struct {
	Type            CommandType `json:"type"`
	Namespace       string      `json:"namespace,omitempty"`
	Space           string      `json:"space,omitempty"`
	Entity          string      `json:"entity,omitempty"`
	NodeID          string      `json:"node_id,omitempty"`
	Addr            string      `json:"addr,omitempty"`
	NowUnix         int64       `json:"now_unix,omitempty"`
	SuspectAfterMS  int64       `json:"suspect_after_ms,omitempty"`
	DeadAfterMS     int64       `json:"dead_after_ms,omitempty"`
	PlanID          uint64      `json:"plan_id,omitempty"`
	TaskID          uint64      `json:"task_id,omitempty"`
	ImportedEntries uint64      `json:"imported_entries,omitempty"`
	DeletedEntries  uint64      `json:"deleted_entries,omitempty"`
	MigrationError  string      `json:"migration_error,omitempty"`
	RetryAfterMS    int64       `json:"retry_after_ms,omitempty"`
}

type ApplyResult struct {
	CreateNamespace controlapi.CreateNamespaceResponse      `json:"create_namespace"`
	CreateSpace     controlapi.CreateSpaceResponse          `json:"create_space"`
	CreateEntity    controlapi.CreateEntityResponse         `json:"create_entity"`
	BumpNamespace   controlapi.BumpNamespaceVersionResponse `json:"bump_namespace"`
	BumpSpace       controlapi.BumpSpaceVersionResponse     `json:"bump_space"`
	RegisterNode    controlapi.RegisterNodeResponse         `json:"register_node"`
	Heartbeat       controlapi.HeartbeatResponse            `json:"heartbeat"`
	Liveness        LivenessResult                          `json:"liveness"`
	MigrationTask   MigrationTaskResult                     `json:"migration_task"`
}

type ControlFSM struct {
	state *ControlState
}

type commandHandler func(context.Context, *ControlFSM, Command) (ApplyResult, error)

var commandHandlers = map[CommandType]commandHandler{
	CommandCreateNamespace:       handleCreateNamespaceCommand,
	CommandCreateSpace:           handleCreateSpaceCommand,
	CommandCreateEntity:          handleCreateEntityCommand,
	CommandBumpNamespace:         handleBumpNamespaceCommand,
	CommandBumpSpace:             handleBumpSpaceCommand,
	CommandRegisterNode:          handleRegisterNodeCommand,
	CommandHeartbeat:             handleHeartbeatCommand,
	CommandAdvanceNodeLiveness:   handleAdvanceNodeLivenessCommand,
	CommandClaimMigrationTask:    handleClaimMigrationTaskCommand,
	CommandCutoverMigrationTask:  handleCutoverMigrationTaskCommand,
	CommandCompleteMigrationTask: handleCompleteMigrationTaskCommand,
	CommandFailMigrationTask:     handleFailMigrationTaskCommand,
}

func NewControlFSM(state *ControlState) *ControlFSM {
	return &ControlFSM{state: state}
}

func (f *ControlFSM) Apply(ctx context.Context, command Command) (ApplyResult, error) {
	handler, ok := commandHandlers[command.Type]
	if !ok {
		return ApplyResult{}, fmt.Errorf("control fsm: unknown command %q", command.Type)
	}
	return handler(ctx, f, command)
}

func handleCreateNamespaceCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyCreateNamespace(command)
}

func handleCreateSpaceCommand(ctx context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyCreateSpace(ctx, command)
}

func handleCreateEntityCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyCreateEntity(command)
}

func handleBumpNamespaceCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyBumpNamespace(command)
}

func handleBumpSpaceCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyBumpSpace(command)
}

func handleRegisterNodeCommand(ctx context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyRegisterNode(ctx, command)
}

func handleHeartbeatCommand(ctx context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyHeartbeat(ctx, command)
}

func handleAdvanceNodeLivenessCommand(ctx context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyAdvanceNodeLiveness(ctx, command)
}

func handleClaimMigrationTaskCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyClaimMigrationTask(command)
}

func handleCutoverMigrationTaskCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyCutoverMigrationTask(command)
}

func handleCompleteMigrationTaskCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyCompleteMigrationTask(command)
}

func handleFailMigrationTaskCommand(_ context.Context, f *ControlFSM, command Command) (ApplyResult, error) {
	return f.applyFailMigrationTask(command)
}

func (f *ControlFSM) applyCreateNamespace(command Command) (ApplyResult, error) {
	response, err := f.state.CreateNamespace(command.Namespace)
	return ApplyResult{CreateNamespace: response}, err
}

func (f *ControlFSM) applyCreateSpace(ctx context.Context, command Command) (ApplyResult, error) {
	response, err := f.state.CreateSpace(ctx, command.Namespace, command.Space)
	return ApplyResult{CreateSpace: response}, err
}

func (f *ControlFSM) applyCreateEntity(command Command) (ApplyResult, error) {
	response, err := f.state.CreateEntity(command.Namespace, command.Space, command.Entity)
	return ApplyResult{CreateEntity: response}, err
}

func (f *ControlFSM) applyBumpNamespace(command Command) (ApplyResult, error) {
	response, err := f.state.BumpNamespaceVersion(command.Namespace)
	return ApplyResult{BumpNamespace: response}, err
}

func (f *ControlFSM) applyBumpSpace(command Command) (ApplyResult, error) {
	response, err := f.state.BumpSpaceVersion(command.Namespace, command.Space)
	return ApplyResult{BumpSpace: response}, err
}

func (f *ControlFSM) applyRegisterNode(ctx context.Context, command Command) (ApplyResult, error) {
	response, err := f.state.registerNodeAt(ctx, command.NodeID, command.Addr, f.commandTime(command))
	return ApplyResult{RegisterNode: response}, err
}

func (f *ControlFSM) applyHeartbeat(ctx context.Context, command Command) (ApplyResult, error) {
	response, err := f.state.heartbeatAt(ctx, command.NodeID, command.Addr, f.commandTime(command))
	return ApplyResult{Heartbeat: response}, err
}

func (f *ControlFSM) applyAdvanceNodeLiveness(ctx context.Context, command Command) (ApplyResult, error) {
	result := f.state.AdvanceLiveness(
		ctx,
		time.Unix(command.NowUnix, 0),
		time.Duration(command.SuspectAfterMS)*time.Millisecond,
		time.Duration(command.DeadAfterMS)*time.Millisecond,
	)
	return ApplyResult{Liveness: result}, nil
}

func (f *ControlFSM) applyClaimMigrationTask(command Command) (ApplyResult, error) {
	result := f.state.ClaimMigrationTask(f.commandTime(command))
	return ApplyResult{MigrationTask: result}, nil
}

func (f *ControlFSM) applyCutoverMigrationTask(command Command) (ApplyResult, error) {
	task, err := f.state.CutoverMigrationTask(
		command.PlanID,
		command.TaskID,
		command.ImportedEntries,
		f.commandTime(command),
	)
	return ApplyResult{MigrationTask: MigrationTaskResult{Claimed: task.State != "", Task: task}}, err
}

func (f *ControlFSM) applyCompleteMigrationTask(command Command) (ApplyResult, error) {
	task, err := f.state.CompleteMigrationTask(
		command.PlanID,
		command.TaskID,
		command.ImportedEntries,
		command.DeletedEntries,
		f.commandTime(command),
	)
	return ApplyResult{MigrationTask: MigrationTaskResult{Claimed: task.State != "", Task: task}}, err
}

func (f *ControlFSM) applyFailMigrationTask(command Command) (ApplyResult, error) {
	task, err := f.state.FailMigrationTask(
		command.PlanID,
		command.TaskID,
		command.MigrationError,
		time.Duration(command.RetryAfterMS)*time.Millisecond,
		f.commandTime(command),
	)
	return ApplyResult{MigrationTask: MigrationTaskResult{Claimed: task.State != "", Task: task}}, err
}

func (f *ControlFSM) commandTime(command Command) time.Time {
	if command.NowUnix > 0 {
		return time.Unix(command.NowUnix, 0)
	}
	return f.state.now()
}
