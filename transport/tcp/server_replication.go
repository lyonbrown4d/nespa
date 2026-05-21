package tcp

import (
	"context"
	"fmt"
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
)

const defaultReplicationTimeout = time.Second

type replicationRequest struct {
	key  cachewire.Key
	send replicationSend
}

func (s *Server) replicateSet(request cachewire.SetRequest) {
	replicaRequest := request
	replicaRequest.Value = append([]byte(nil), request.Value...)
	replicaRequest.RouteEpoch = 0
	replicaRequest.ExpectedVersion = 0
	s.replicate(replicationRequest{
		key: request.Key,
		send: func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.Set(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate set: %w", err)
			}
			return nil
		},
	})
}

func (s *Server) replicateDelete(request cachewire.DeleteRequest) {
	replicaRequest := request
	replicaRequest.RouteEpoch = 0
	replicaRequest.ExpectedVersion = 0
	s.replicate(replicationRequest{
		key: request.Key,
		send: func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.Delete(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate delete: %w", err)
			}
			return nil
		},
	})
}

func (s *Server) replicateTouch(request cachewire.TouchRequest) {
	replicaRequest := request
	replicaRequest.RouteEpoch = 0
	replicaRequest.ExpectedVersion = 0
	s.replicate(replicationRequest{
		key: request.Key,
		send: func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.Touch(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate touch: %w", err)
			}
			return nil
		},
	})
}

func (s *Server) replicateAdjust(request cachewire.AdjustRequest) {
	replicaRequest := request
	replicaRequest.RouteEpoch = 0
	replicaRequest.ExpectedVersion = 0
	s.replicate(replicationRequest{
		key: request.Key,
		send: func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.Adjust(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate adjust: %w", err)
			}
			return nil
		},
	})
}

func (s *Server) replicatePrimitive(request cachewire.PrimitiveRequest) {
	replicaRequest := replicaPrimitiveRequest(request)
	s.replicate(replicationRequest{
		key: request.Key,
		send: func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.Primitive(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate primitive: %w", err)
			}
			return nil
		},
	})
}

func (s *Server) replicateBatchSet(request cachewire.BatchSetRequest, results []cache.SetResult) {
	batches := make(map[string][]cachewire.SetRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Found {
			continue
		}
		item := replicaSetRequest(request.Items[index])
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		replicaRequest := cachewire.BatchSetRequest{Items: items}
		s.replication.Enqueue(target, func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.BatchSet(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate batch set: %w", err)
			}
			return nil
		})
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
		item := replicaDeleteRequest(request.Items[index])
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		replicaRequest := cachewire.BatchDeleteRequest{Items: items}
		s.replication.Enqueue(target, func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.BatchDelete(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate batch delete: %w", err)
			}
			return nil
		})
	}
}

func (s *Server) replicateBatchTouch(request cachewire.BatchTouchRequest, results []cache.TouchResult) {
	batches := make(map[string][]cachewire.TouchRequest)
	for index := range min(len(request.Items), len(results)) {
		if !results[index].Touched {
			continue
		}
		item := replicaTouchRequest(request.Items[index])
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		replicaRequest := cachewire.BatchTouchRequest{Items: items}
		s.replication.Enqueue(target, func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.BatchTouch(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate batch touch: %w", err)
			}
			return nil
		})
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
		item := replicaPrimitiveRequest(request.Items[index])
		addReplicaBatchItem(s, batches, item.Key, item)
	}
	for target, items := range batches {
		replicaRequest := cachewire.BatchPrimitiveRequest{Items: items}
		s.replication.Enqueue(target, func(replicationCtx context.Context, client *Client, target string) error {
			if _, err := client.BatchPrimitive(replicationCtx, target, replicaRequest); err != nil {
				return fmt.Errorf("replicate batch primitive: %w", err)
			}
			return nil
		})
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
		s.replication.Enqueue(target, request.send)
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
