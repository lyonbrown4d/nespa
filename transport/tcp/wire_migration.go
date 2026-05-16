package tcp

import (
	"time"

	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
)

func rangeFromWire(request cachewire.MigrationRangeRequest) cache.RangeOptions {
	return cache.RangeOptions{
		Namespace:  request.Namespace,
		Space:      request.Space,
		VSlotStart: request.VSlotStart,
		VSlotEnd:   request.VSlotEnd,
	}
}

func snapshotToWire(snapshot cache.Snapshot) cachewire.MigrationSnapshot {
	entries := make([]cachewire.MigrationSnapshotEntry, 0, len(snapshot.Entries))
	for index := range snapshot.Entries {
		entries = append(entries, snapshotEntryToWire(snapshot.Entries[index]))
	}
	return cachewire.MigrationSnapshot{Entries: entries}
}

func snapshotFromWire(snapshot cachewire.MigrationSnapshot) cache.Snapshot {
	entries := make([]cache.SnapshotEntry, 0, len(snapshot.Entries))
	for index := range snapshot.Entries {
		entries = append(entries, snapshotEntryFromWire(snapshot.Entries[index]))
	}
	return cache.Snapshot{Entries: entries}
}

func snapshotEntryToWire(entry cache.SnapshotEntry) cachewire.MigrationSnapshotEntry {
	return cachewire.MigrationSnapshotEntry{
		Key: cachewire.Key{
			Namespace: entry.Key.Namespace,
			Space:     entry.Key.Space,
			Entity:    entry.Key.Entity,
			Key:       entry.Key.Key,
		},
		Value:              entry.Value,
		Version:            entry.Version,
		NamespaceVersion:   entry.NamespaceVersion,
		SpaceVersion:       entry.SpaceVersion,
		ExpireAtUnixMs:     timeToUnixMilli(entry.ExpireAt),
		CreatedAtUnixMs:    timeToUnixMilli(entry.CreatedAt),
		UpdatedAtUnixMs:    timeToUnixMilli(entry.UpdatedAt),
		LastAccessAtUnixMs: timeToUnixMilli(entry.LastAccessAt),
		AccessCount:        entry.AccessCount,
	}
}

func snapshotEntryFromWire(entry cachewire.MigrationSnapshotEntry) cache.SnapshotEntry {
	return cache.SnapshotEntry{
		Key: cache.Key{
			Namespace: entry.Namespace,
			Space:     entry.Space,
			Entity:    entry.Entity,
			Key:       entry.Key.Key,
		},
		Value:            entry.Value,
		Version:          entry.Version,
		NamespaceVersion: entry.NamespaceVersion,
		SpaceVersion:     entry.SpaceVersion,
		ExpireAt:         timeFromUnixMilli(entry.ExpireAtUnixMs),
		CreatedAt:        timeFromUnixMilli(entry.CreatedAtUnixMs),
		UpdatedAt:        timeFromUnixMilli(entry.UpdatedAtUnixMs),
		LastAccessAt:     timeFromUnixMilli(entry.LastAccessAtUnixMs),
		AccessCount:      entry.AccessCount,
	}
}

func timeToUnixMilli(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func timeFromUnixMilli(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}
