package tcp

import (
	"sync"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/routing"
)

type rangeFence struct {
	namespace string
	space     string
	start     uint32
	end       uint32
}

type rangeFenceSet struct {
	mu     sync.RWMutex
	ranges []rangeFence
}

func newRangeFenceSet() *rangeFenceSet {
	return &rangeFenceSet{}
}

func rangeFenceFromWire(request cachewire.MigrationRangeRequest) rangeFence {
	return rangeFence{
		namespace: request.Namespace,
		space:     request.Space,
		start:     request.VSlotStart,
		end:       request.VSlotEnd,
	}
}

func (s *rangeFenceSet) add(fence rangeFence) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for index := range s.ranges {
		if sameRangeFence(s.ranges[index], fence) {
			return
		}
	}
	s.ranges = append(s.ranges, fence)
}

func (s *rangeFenceSet) remove(fence rangeFence) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.ranges[:0]
	for index := range s.ranges {
		if !sameRangeFence(s.ranges[index], fence) {
			next = append(next, s.ranges[index])
		}
	}
	s.ranges = next
}

func (s *rangeFenceSet) containsKey(key cachewire.Key) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for index := range s.ranges {
		if keyInRangeFence(key, s.ranges[index]) {
			return true
		}
	}
	return false
}

func (s *rangeFenceSet) empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ranges) == 0
}

func keyInRangeFence(key cachewire.Key, fence rangeFence) bool {
	if key.Namespace != fence.namespace || key.Space != fence.space {
		return false
	}
	slot := routing.VSlotFor(key.Namespace, key.Space, key.Key)
	return slot >= fence.start && slot <= fence.end
}

func sameRangeFence(left, right rangeFence) bool {
	return left.namespace == right.namespace &&
		left.space == right.space &&
		left.start == right.start &&
		left.end == right.end
}
