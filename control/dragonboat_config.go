package control

import (
	"fmt"
	"strings"

	dragonboat "github.com/lni/dragonboat/v3"
	"github.com/samber/oops"
)

func normalizeRaftConfig(cfg RaftConfig) (RaftConfig, error) {
	cfg = applyRaftConfigDefaults(cfg)
	if err := validateRaftSelfConfig(cfg); err != nil {
		return RaftConfig{}, err
	}
	if err := normalizeRaftMembers(cfg.Members); err != nil {
		return RaftConfig{}, err
	}
	return cfg, nil
}

func applyRaftConfigDefaults(cfg RaftConfig) RaftConfig {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	if cfg.Addr == "" {
		cfg.Addr = defaultControlRaftAddr
	}
	if cfg.ClusterID == 0 {
		cfg.ClusterID = defaultControlRaftClusterID
	}
	if cfg.NodeID == 0 {
		cfg.NodeID = defaultControlRaftNodeID
	}
	if cfg.ProposalTimeout <= 0 {
		cfg.ProposalTimeout = defaultControlProposalWait
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = defaultControlSnapshotEntries
	}
	if cfg.CompactionOverhead == 0 {
		cfg.CompactionOverhead = defaultControlCompactionOverhead
	}
	return cfg
}

func validateRaftSelfConfig(cfg RaftConfig) error {
	if cfg.NodeID == 0 {
		return fmt.Errorf("control raft node id invalid: %w", oops.Code("control_raft_invalid_node_id").
			In("control.raft").
			New("control raft node id must be greater than zero"))
	}
	if _, err := normalizeNodeAddr(cfg.Addr); err != nil {
		return oops.Code("control_raft_invalid_addr").
			In("control.raft").
			With("addr", cfg.Addr).
			Wrapf(err, "normalize control raft addr")
	}
	return nil
}

func normalizeRaftMembers(members []RaftMember) error {
	for i := range members {
		members[i].Addr = strings.TrimSpace(members[i].Addr)
		if members[i].NodeID == 0 {
			return fmt.Errorf("control raft member invalid: %w", oops.Code("control_raft_invalid_member").
				In("control.raft").
				With("index", i).
				New("control raft member node_id must be greater than zero"))
		}
		if _, err := normalizeNodeAddr(members[i].Addr); err != nil {
			return oops.Code("control_raft_invalid_member").
				In("control.raft").
				With("node_id", members[i].NodeID).
				With("addr", members[i].Addr).
				Wrapf(err, "normalize control raft member")
		}
	}
	return nil
}

func resolveRaftInitialMembers(cfg RaftConfig, hasData bool) map[uint64]dragonboat.Target {
	if hasData || cfg.Join {
		return nil
	}
	members := make(map[uint64]dragonboat.Target, len(cfg.Members)+1)
	for _, member := range cfg.Members {
		members[member.NodeID] = member.Addr
	}
	members[cfg.NodeID] = cfg.Addr
	return members
}
