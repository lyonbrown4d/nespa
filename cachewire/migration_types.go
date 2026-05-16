package cachewire

type MigrationRangeRequest struct {
	Namespace  string `json:"namespace"`
	Space      string `json:"space"`
	VSlotStart uint32 `json:"vslot_start"`
	VSlotEnd   uint32 `json:"vslot_end"`
	RouteEpoch uint64 `json:"-"`
}

type MigrationSnapshot struct {
	Entries []MigrationSnapshotEntry `json:"entries"`
}

type MigrationSnapshotEntry struct {
	Key
	Value              []byte `json:"-"`
	Version            uint64 `json:"version"`
	NamespaceVersion   uint64 `json:"namespace_version"`
	SpaceVersion       uint64 `json:"space_version"`
	ExpireAtUnixMs     int64  `json:"expire_at_ms,omitempty"`
	CreatedAtUnixMs    int64  `json:"created_at_ms,omitempty"`
	UpdatedAtUnixMs    int64  `json:"updated_at_ms,omitempty"`
	LastAccessAtUnixMs int64  `json:"last_access_at_ms,omitempty"`
	AccessCount        uint64 `json:"access_count,omitempty"`
	PayloadOffset      uint32 `json:"payload_offset,omitempty"`
	PayloadSize        uint32 `json:"payload_size,omitempty"`
}

type MigrationImportResponse struct {
	Imported uint64 `json:"imported"`
}

type MigrationDeleteRangeResponse struct {
	Deleted uint64 `json:"deleted"`
}
