package engine

import (
	"encoding/binary"
	"time"
)

func newEntry(cmd shardCommand, expireAt time.Time, cost uint64) *entry {
	return &entry{
		key:              cmd.key,
		value:            cmd.value,
		version:          1,
		namespaceVersion: cmd.setOpts.NamespaceVersion,
		spaceVersion:     cmd.setOpts.SpaceVersion,
		expireAt:         expireAt,
		createdAt:        cmd.now,
		updatedAt:        cmd.now,
		lastAccessAt:     cmd.now,
		accessCount:      1,
		costBytes:        cost,
	}
}

func (e *entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !e.expireAt.After(now)
}

func (e *entry) visible(opts GetOptions) bool {
	if opts.NamespaceVersion != 0 && e.namespaceVersion != opts.NamespaceVersion {
		return false
	}
	if opts.SpaceVersion != 0 && e.spaceVersion != opts.SpaceVersion {
		return false
	}
	return true
}

func (e *entry) record() Record {
	return Record{
		Key:              e.key,
		Value:            append([]byte(nil), e.value...),
		CostBytes:        e.costBytes,
		Version:          e.version,
		NamespaceVersion: e.namespaceVersion,
		SpaceVersion:     e.spaceVersion,
		ExpireAt:         e.expireAt,
		CreatedAt:        e.createdAt,
		UpdatedAt:        e.updatedAt,
		LastAccessAt:     e.lastAccessAt,
		AccessCount:      e.accessCount,
	}
}

func validateKey(key Key) error {
	if key.Namespace == "" || key.Space == "" || key.Key == "" {
		return ErrInvalidKey
	}
	return nil
}

func physicalKey(key Key) string {
	var raw []byte
	raw = appendKeyPart(raw, key.Namespace)
	raw = appendKeyPart(raw, key.Space)
	raw = appendKeyPart(raw, key.Entity)
	raw = appendKeyPart(raw, key.Key)
	return string(raw)
}

func appendKeyPart(raw []byte, part string) []byte {
	raw = binary.AppendUvarint(raw, uint64(len(part)))
	return append(raw, part...)
}

func spaceKeyOf(key Key) spaceKey {
	return spaceKey{namespace: key.Namespace, space: key.Space}
}

func EstimateCost(key Key, value []byte) uint64 {
	return costOf(key, value)
}

func costOf(key Key, value []byte) uint64 {
	return uint64(len(key.Namespace) + len(key.Space) + len(key.Entity) + len(key.Key) + len(value))
}
