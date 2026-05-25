package tcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lyonbrown4d/nespa/protocol"
	"github.com/samber/oops"
)

type replicationOutboxEntry struct {
	Sequence    uint64                 `json:"sequence"`
	Target      string                 `json:"target"`
	Kind        replicationCommandKind `json:"kind"`
	Op          protocol.Op            `json:"op"`
	Metadata    []byte                 `json:"metadata"`
	Payload     []byte                 `json:"payload,omitempty"`
	CreatedUnix int64                  `json:"created_unix_ms"`
}

type replicationOutbox struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
}

type replicationOutboxSnapshot struct {
	entries     uint64
	maxSequence uint64
}

func scanReplicationOutbox(path string) (replicationOutboxSnapshot, error) {
	dir, name, err := replicationOutboxDirAndName(path)
	if err != nil {
		return replicationOutboxSnapshot{}, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return replicationOutboxSnapshot{}, nil
		}
		return replicationOutboxSnapshot{}, fmt.Errorf("open replication outbox directory: %w", err)
	}
	defer closeReplicationOutboxRoot(root)

	file, err := root.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return replicationOutboxSnapshot{}, nil
		}
		return replicationOutboxSnapshot{}, fmt.Errorf("open replication outbox for scan: %w", err)
	}
	defer closeReplicationOutboxFile(file)

	return readReplicationOutboxSnapshot(file)
}

func scanReplicationOutboxEntries(path string) ([]replicationOutboxEntry, error) {
	dir, name, err := replicationOutboxDirAndName(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open replication outbox directory: %w", err)
	}
	defer closeReplicationOutboxRoot(root)

	file, err := root.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open replication outbox for scan: %w", err)
	}
	defer closeReplicationOutboxFile(file)

	return readReplicationOutboxEntries(file)
}

func readReplicationOutboxEntries(reader io.Reader) ([]replicationOutboxEntry, error) {
	decoder := json.NewDecoder(reader)
	entries := make([]replicationOutboxEntry, 0)
	for {
		var entry replicationOutboxEntry
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode replication outbox entry: %w", err)
		}
		entries = append(entries, entry)
	}
}

func replayableReplicationOutboxSnapshot(entries []replicationOutboxEntry) replicationOutboxSnapshot {
	var snapshot replicationOutboxSnapshot
	for index := range entries {
		entry := entries[index]
		snapshot.entries++
		if entry.Sequence > snapshot.maxSequence {
			snapshot.maxSequence = entry.Sequence
		}
	}
	return snapshot
}

func openReplicationOutbox(path string) (*replicationOutbox, error) {
	dir, name, err := replicationOutboxDirAndName(path)
	if err != nil {
		return nil, err
	}
	if mkdirErr := os.MkdirAll(dir, 0o750); mkdirErr != nil {
		return nil, fmt.Errorf("create replication outbox directory: %w", mkdirErr)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open replication outbox directory: %w", err)
	}
	defer closeReplicationOutboxRoot(root)
	file, err := root.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open replication outbox: %w", err)
	}
	return &replicationOutbox{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func replicationOutboxEnabled(path string) bool {
	return strings.TrimSpace(path) != ""
}

func replicationOutboxDirAndName(path string) (string, string, error) {
	clean := filepath.Clean(strings.TrimSpace(path))
	dir, name := filepath.Split(clean)
	if name == "" || name == "." {
		err := oops.Code("invalid_replication_outbox_path").
			In("transport.tcp").
			With("path", path).
			New("cache tcp: invalid replication outbox path")
		return "", "", fmt.Errorf("validate replication outbox path: %w", err)
	}
	if dir == "" {
		dir = "."
	}
	return dir, name, nil
}

func readReplicationOutboxSnapshot(reader io.Reader) (replicationOutboxSnapshot, error) {
	var snapshot replicationOutboxSnapshot
	decoder := json.NewDecoder(reader)
	for {
		var entry replicationOutboxEntry
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			return snapshot, nil
		}
		if err != nil {
			return replicationOutboxSnapshot{}, fmt.Errorf("decode replication outbox entry: %w", err)
		}
		snapshot.entries++
		if entry.Sequence > snapshot.maxSequence {
			snapshot.maxSequence = entry.Sequence
		}
	}
}

func closeReplicationOutboxRoot(root *os.Root) {
	if err := root.Close(); err != nil {
		return
	}
}

func closeReplicationOutboxFile(file *os.File) {
	if err := file.Close(); err != nil {
		return
	}
}

func (o *replicationOutbox) Append(entry replicationOutboxEntry) error {
	if o == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if err := o.encoder.Encode(entry); err != nil {
		return fmt.Errorf("append replication outbox entry: %w", err)
	}
	if err := o.file.Sync(); err != nil {
		return fmt.Errorf("sync replication outbox entry: %w", err)
	}
	return nil
}

func (o *replicationOutbox) Close() error {
	if o == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if err := o.file.Close(); err != nil {
		return fmt.Errorf("close replication outbox: %w", err)
	}
	return nil
}

func newReplicationOutboxEntry(sequence uint64, target string, command replicationCommand) (replicationOutboxEntry, error) {
	frame, err := command.encodeFrame()
	if err != nil {
		return replicationOutboxEntry{}, err
	}
	return replicationOutboxEntry{
		Sequence:    sequence,
		Target:      target,
		Kind:        command.kind,
		Op:          frame.op,
		Metadata:    frame.metadata,
		Payload:     frame.payload,
		CreatedUnix: time.Now().UnixMilli(),
	}, nil
}
