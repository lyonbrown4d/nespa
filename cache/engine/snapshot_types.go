package engine

import "time"

type Snapshot struct {
	Entries []SnapshotEntry `json:"entries"`
}

type SnapshotEntry struct {
	Key              Key       `json:"key"`
	Value            []byte    `json:"value"`
	Version          uint64    `json:"version"`
	NamespaceVersion uint64    `json:"namespace_version"`
	SpaceVersion     uint64    `json:"space_version"`
	ExpireAt         time.Time `json:"expire_at"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	LastAccessAt     time.Time `json:"last_access_at"`
	AccessCount      uint64    `json:"access_count"`
}
