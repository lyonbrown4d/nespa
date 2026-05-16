package nespa

import (
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

type ErrorCode = protocol.ErrorCode

const (
	ErrorUnknown            = protocol.ErrorUnknown
	ErrorBadFrame           = protocol.ErrorBadFrame
	ErrorUnsupportedVersion = protocol.ErrorUnsupportedVersion
	ErrorTooLarge           = protocol.ErrorTooLarge
	ErrorNoRoute            = protocol.ErrorNoRoute
	ErrorTimeout            = protocol.ErrorTimeout
	ErrorUnavailable        = protocol.ErrorUnavailable
	ErrorInternal           = protocol.ErrorInternal
	ErrorInvalidArgument    = protocol.ErrorInvalidArgument
)

type Key struct {
	Namespace string
	Space     string
	Entity    string
	Key       string
}

type SetOptions struct {
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type GetOptions struct {
	NamespaceVersion uint64
	SpaceVersion     uint64
}

type DeleteOptions struct {
	ExpectedVersion uint64
}

type TouchOptions struct {
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type AdjustOptions struct {
	Delta            int64
	InitialValue     int64
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type Record struct {
	Found            bool
	Key              Key
	Value            []byte
	Version          uint64
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpireAt         time.Time
}

type SetItem struct {
	Key     Key
	Value   []byte
	Options SetOptions
}

type GetItem struct {
	Key     Key
	Options GetOptions
}

type DeleteItem struct {
	Key     Key
	Options DeleteOptions
}

type TouchItem struct {
	Key     Key
	Options TouchOptions
}

func wireKey(key Key) cachewire.Key {
	return cachewire.Key{
		Namespace: key.Namespace,
		Space:     key.Space,
		Entity:    key.Entity,
		Key:       key.Key,
	}
}

func recordFromWire(record cachewire.Record) Record {
	out := Record{
		Found: record.Found,
		Key: Key{
			Namespace: record.Namespace,
			Space:     record.Space,
			Entity:    record.Entity,
			Key:       record.Key,
		},
		Value:            append([]byte(nil), record.Value...),
		Version:          record.Version,
		NamespaceVersion: record.NamespaceVersion,
		SpaceVersion:     record.SpaceVersion,
	}
	if record.ExpireAtUnixMs > 0 {
		out.ExpireAt = time.UnixMilli(record.ExpireAtUnixMs)
	}
	return out
}

func ttlMillis(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return ttl.Milliseconds()
}
