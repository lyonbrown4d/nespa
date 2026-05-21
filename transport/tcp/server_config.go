package tcp

import (
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

type ServerConfig struct {
	Addr                  string
	CurrentRouteEpoch     func() uint64
	ReplicaTargets        func(cachewire.Key) []string
	ReplicationOutboxPath string
}

func NewServer(cfg ServerConfig, service cache.Service) *Server {
	return &Server{
		addr:              cfg.Addr,
		service:           service,
		codec:             protocol.NewCodec(),
		currentRouteEpoch: cfg.CurrentRouteEpoch,
		replicaTargets:    cfg.ReplicaTargets,
		replication:       newReplicationDispatcher(NewClient(), defaultReplicationTimeout, defaultReplicationQueueSize),
		replicationOutbox: cfg.ReplicationOutboxPath,
		fences:            newRangeFenceSet(),
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) ReplicationStats() ReplicationStats {
	if s.replication == nil {
		return ReplicationStats{}
	}
	return s.replication.Stats()
}
