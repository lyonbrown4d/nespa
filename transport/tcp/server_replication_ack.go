package tcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

const replicationAckSuffix = ".acks.json"

type replicationAckState struct {
	Offsets map[string]uint64 `json:"offsets"`
}

type replicationAckSnapshot struct {
	targets     uint64
	maxSequence uint64
}

type replicationAckStore struct {
	mu      sync.Mutex
	dir     string
	name    string
	offsets *collectionmapping.Map[string, uint64]
}

func openReplicationAckStore(outboxPath string) (*replicationAckStore, error) {
	dir, name, err := replicationAckDirAndName(outboxPath)
	if err != nil {
		return nil, err
	}
	store := &replicationAckStore{
		dir:     dir,
		name:    name,
		offsets: collectionmapping.NewMap[string, uint64](),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func replicationAckDirAndName(outboxPath string) (string, string, error) {
	dir, name, err := replicationOutboxDirAndName(outboxPath)
	if err != nil {
		return "", "", err
	}
	return dir, name + replicationAckSuffix, nil
}

func (s *replicationAckStore) Ack(target string, sequence uint64) error {
	if s == nil || strings.TrimSpace(target) == "" || sequence == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.offsets.Get(target)
	if ok && current >= sequence {
		return nil
	}
	s.offsets.Set(target, sequence)
	if err := s.saveLocked(); err != nil {
		return err
	}
	return nil
}

func (s *replicationAckStore) Snapshot() replicationAckSnapshot {
	if s == nil {
		return replicationAckSnapshot{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var snapshot replicationAckSnapshot
	s.offsets.Range(func(_ string, sequence uint64) bool {
		snapshot.targets++
		if sequence > snapshot.maxSequence {
			snapshot.maxSequence = sequence
		}
		return true
	})
	return snapshot
}

func (s *replicationAckStore) load() error {
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open replication ack directory: %w", err)
	}
	defer closeReplicationOutboxRoot(root)

	raw, err := root.ReadFile(s.name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read replication ack state: %w", err)
	}
	var state replicationAckState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fmt.Errorf("decode replication ack state: %w", err)
	}
	s.offsets = collectionmapping.NewMapFrom(state.Offsets)
	return nil
}

func (s *replicationAckStore) saveLocked() error {
	raw, err := json.MarshalIndent(replicationAckState{Offsets: s.offsets.All()}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode replication ack state: %w", err)
	}
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return fmt.Errorf("open replication ack directory: %w", err)
	}
	defer closeReplicationOutboxRoot(root)

	tmp := "." + s.name + ".tmp"
	if err := root.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write replication ack temp state: %w", err)
	}
	if err := root.Rename(tmp, s.name); err != nil {
		return errors.Join(
			fmt.Errorf("write replication ack state: %w", err),
			removeReplicationAckTemp(root, tmp),
		)
	}
	return nil
}

func removeReplicationAckTemp(root *os.Root, name string) error {
	if err := root.Remove(filepath.Clean(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove replication ack temp state: %w", err)
	}
	return nil
}
