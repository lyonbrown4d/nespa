package tcp

import (
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
)

const defaultReplicationTimeout = time.Second

type replicationRequest struct {
	key     cachewire.Key
	command replicationCommand
}

func (s *Server) replicateSet(request cachewire.SetRequest) {
	s.replicate(replicationRequest{
		key:     request.Key,
		command: newSetReplicationCommand(request),
	})
}

func (s *Server) replicateDelete(request cachewire.DeleteRequest) {
	s.replicate(replicationRequest{
		key:     request.Key,
		command: newDeleteReplicationCommand(request),
	})
}

func (s *Server) replicateTouch(request cachewire.TouchRequest) {
	s.replicate(replicationRequest{
		key:     request.Key,
		command: newTouchReplicationCommand(request),
	})
}

func (s *Server) replicateAdjust(request cachewire.AdjustRequest) {
	s.replicate(replicationRequest{
		key:     request.Key,
		command: newAdjustReplicationCommand(request),
	})
}

func (s *Server) replicatePrimitive(request cachewire.PrimitiveRequest) {
	s.replicate(replicationRequest{
		key:     request.Key,
		command: newPrimitiveReplicationCommand(request),
	})
}

func (s *Server) replicateBatchSet(request cachewire.BatchSetRequest, results []cache.SetResult) {
	batches := make(map[string][]cachewire.SetRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Found {
			continue
		}
		item := request.Items[index]
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		command := newBatchSetReplicationCommand(cachewire.BatchSetRequest{Items: items})
		s.replication.Enqueue(target, command)
	}
}

func (s *Server) replicateBatchDelete(
	request cachewire.BatchDeleteRequest,
	results []cache.DeleteResult,
) {
	batches := make(map[string][]cachewire.DeleteRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Deleted {
			continue
		}
		item := request.Items[index]
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		command := newBatchDeleteReplicationCommand(cachewire.BatchDeleteRequest{Items: items})
		s.replication.Enqueue(target, command)
	}
}

func (s *Server) replicateBatchTouch(request cachewire.BatchTouchRequest, results []cache.TouchResult) {
	batches := make(map[string][]cachewire.TouchRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Touched {
			continue
		}
		item := request.Items[index]
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		command := newBatchTouchReplicationCommand(cachewire.BatchTouchRequest{Items: items})
		s.replication.Enqueue(target, command)
	}
}

func (s *Server) replicateBatchPrimitive(
	request cachewire.BatchPrimitiveRequest,
	results []cache.PrimitiveResult,
) {
	batches := make(map[string][]cachewire.PrimitiveRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Applied || !cache.PrimitiveKind(request.Items[index].Kind).Mutates() {
			continue
		}
		item := request.Items[index]
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		command := newBatchPrimitiveReplicationCommand(cachewire.BatchPrimitiveRequest{Items: items})
		s.replication.Enqueue(target, command)
	}
}

func (s *Server) replicate(request replicationRequest) {
	if s.replicaTargets == nil || s.replication == nil {
		return
	}

	targets := s.replicaTargets(request.key)
	if len(targets) == 0 {
		return
	}

	for index := range targets {
		target := targets[index]
		s.replication.Enqueue(target, request.command)
	}
}

func addReplicaBatchItem[T any](s *Server, batches map[string][]T, key cachewire.Key, item T) {
	if s.replicaTargets == nil || s.replication == nil {
		return
	}
	targets := s.replicaTargets(key)
	for index := range targets {
		target := targets[index]
		if target == "" {
			continue
		}
		batches[target] = append(batches[target], item)
	}
}

func replicaSetRequest(request cachewire.SetRequest) cachewire.SetRequest {
	request.Value = append([]byte(nil), request.Value...)
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return request
}

func replicaDeleteRequest(request cachewire.DeleteRequest) cachewire.DeleteRequest {
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return request
}

func replicaTouchRequest(request cachewire.TouchRequest) cachewire.TouchRequest {
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return request
}

func replicaPrimitiveRequest(request cachewire.PrimitiveRequest) cachewire.PrimitiveRequest {
	request.Value = append([]byte(nil), request.Value...)
	request.RouteEpoch = 0
	request.ExpectedVersion = 0
	return request
}
