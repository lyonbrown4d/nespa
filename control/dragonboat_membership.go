package control

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/samber/oops"
)

func (r *DragonboatRuntime) AddRaftNode(ctx context.Context, nodeID uint64, addr string) error {
	addr = strings.TrimSpace(addr)
	if nodeID == 0 {
		return newControlRaftNodeInvalidError()
	}
	if _, err := normalizeNodeAddr(addr); err != nil {
		return oops.Code("control_raft_member_invalid").In("control.raft").Wrapf(err, "control raft member addr invalid")
	}

	_, err := dragonboatRetry(ctx, r.proposalTimeout, func(attemptCtx context.Context) (struct{}, error) {
		index, err := r.configChangeIndex(attemptCtx)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, wrapDragonboatRawError(
			r.nodeHost.SyncRequestAddNode(attemptCtx, r.clusterID, nodeID, addr, index),
		)
	})
	return err
}

func (r *DragonboatRuntime) RemoveRaftNode(ctx context.Context, nodeID uint64) error {
	if nodeID == 0 {
		return newControlRaftNodeInvalidError()
	}

	_, err := dragonboatRetry(ctx, r.proposalTimeout, func(attemptCtx context.Context) (struct{}, error) {
		index, err := r.configChangeIndex(attemptCtx)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, wrapDragonboatRawError(
			r.nodeHost.SyncRequestDeleteNode(attemptCtx, r.clusterID, nodeID, index),
		)
	})
	return err
}

func (r *DragonboatRuntime) RaftMembers(ctx context.Context) (map[uint64]string, error) {
	return dragonboatRetry(ctx, r.proposalTimeout, func(attemptCtx context.Context) (map[uint64]string, error) {
		membership, err := r.nodeHost.SyncGetClusterMembership(attemptCtx, r.clusterID)
		if err != nil {
			return nil, wrapDragonboatRawError(err)
		}

		members := make(map[uint64]string, len(membership.Nodes)+len(membership.Observers)+len(membership.Witnesses))
		maps.Copy(members, membership.Nodes)
		maps.Copy(members, membership.Observers)
		maps.Copy(members, membership.Witnesses)
		return members, nil
	})
}

func (r *DragonboatRuntime) configChangeIndex(ctx context.Context) (uint64, error) {
	membership, err := r.nodeHost.SyncGetClusterMembership(ctx, r.clusterID)
	if err != nil {
		return 0, wrapDragonboatRawError(err)
	}
	return membership.ConfigChangeID, nil
}

func newControlRaftNodeInvalidError() error {
	return fmt.Errorf("control raft node invalid: %w", oops.Code("control_raft_node_invalid").
		In("control.raft").
		New("control raft node id must be greater than zero"))
}
