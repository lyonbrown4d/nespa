package control

import (
	"context"
	"fmt"

	"github.com/samber/oops"
)

func (s *ServiceRuntime) AddRaftNode(ctx context.Context, nodeID uint64, addr string) error {
	raft := s.dragonboatRuntime()
	if raft == nil {
		return controlRaftNotAvailableError()
	}
	return raft.AddRaftNode(ctx, nodeID, addr)
}

func (s *ServiceRuntime) RemoveRaftNode(ctx context.Context, nodeID uint64) error {
	raft := s.dragonboatRuntime()
	if raft == nil {
		return controlRaftNotAvailableError()
	}
	return raft.RemoveRaftNode(ctx, nodeID)
}

func (s *ServiceRuntime) RaftMembers(ctx context.Context) (map[uint64]string, error) {
	raft := s.dragonboatRuntime()
	if raft == nil {
		return nil, controlRaftNotAvailableError()
	}
	return raft.RaftMembers(ctx)
}

func controlRaftNotAvailableError() error {
	return fmt.Errorf("control raft not available: %w", oops.Code("control_raft_not_available").
		In("control").
		New("control raft runtime is not started"))
}
