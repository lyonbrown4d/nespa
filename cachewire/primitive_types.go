package cachewire

type PrimitiveKind uint8

const (
	PrimitiveCounterAdjust PrimitiveKind = iota + 1
	PrimitiveMapSet
	PrimitiveMapGet
	PrimitiveMapDelete
	PrimitiveMapGetAll
	PrimitiveSetAdd
	PrimitiveSetRemove
	PrimitiveSetContains
	PrimitiveSetMembers
	PrimitiveScoredSetPut
	PrimitiveScoredSetRemove
	PrimitiveScoredSetRange
	PrimitiveListPushFront
	PrimitiveListPushBack
	PrimitiveListPopFront
	PrimitiveListPopBack
	PrimitiveListRange
	PrimitiveBitmapSetBit
	PrimitiveBitmapGetBit
	PrimitiveBitmapBitCount
	PrimitiveHLLAdd
	PrimitiveHLLCount
	PrimitiveHLLMerge
	PrimitiveHLLMembers
	PrimitiveGeoAdd
	PrimitiveGeoDist
	PrimitiveGeoRadius
)

type PrimitiveRequest struct {
	Key
	RouteEpoch       uint64        `json:"-"`
	Kind             PrimitiveKind `json:"kind"`
	TTLMillis        int64         `json:"ttl_ms,omitempty"`
	NamespaceVersion uint64        `json:"namespace_version,omitempty"`
	SpaceVersion     uint64        `json:"space_version,omitempty"`
	ExpectedVersion  uint64        `json:"expected_version,omitempty"`
	Field            string        `json:"field,omitempty"`
	Member           string        `json:"member,omitempty"`
	Value            []byte        `json:"-"`
	Delta            int64         `json:"delta,omitempty"`
	InitialValue     int64         `json:"initial_value,omitempty"`
	Score            float64       `json:"score,omitempty"`
	MinScore         float64       `json:"min_score,omitempty"`
	MaxScore         float64       `json:"max_score,omitempty"`
	HasMinScore      bool          `json:"has_min_score,omitempty"`
	HasMaxScore      bool          `json:"has_max_score,omitempty"`
	Limit            uint64        `json:"limit,omitempty"`
	Start            int64         `json:"start,omitempty"`
	Reverse          bool          `json:"reverse,omitempty"`
	PayloadOffset    uint32        `json:"payload_offset,omitempty"`
	PayloadSize      uint32        `json:"payload_size,omitempty"`
}

type PrimitiveResult struct {
	Record        Record         `json:"record"`
	Found         bool           `json:"found"`
	Applied       bool           `json:"applied"`
	Value         []byte         `json:"-"`
	Bool          bool           `json:"bool,omitempty"`
	Count         uint64         `json:"count,omitempty"`
	Fields        []MapField     `json:"fields,omitempty"`
	Members       []string       `json:"members,omitempty"`
	ScoredMembers []ScoredMember `json:"scored_members,omitempty"`
	Values        []ListValue    `json:"values,omitempty"`
	PayloadOffset uint32         `json:"payload_offset,omitempty"`
	PayloadSize   uint32         `json:"payload_size,omitempty"`
}

type MapField struct {
	Field         string `json:"field"`
	Value         []byte `json:"-"`
	PayloadOffset uint32 `json:"payload_offset,omitempty"`
	PayloadSize   uint32 `json:"payload_size,omitempty"`
}

type ScoredMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type ListValue struct {
	Value         []byte `json:"-"`
	PayloadOffset uint32 `json:"payload_offset,omitempty"`
	PayloadSize   uint32 `json:"payload_size,omitempty"`
}

type BatchPrimitiveRequest struct {
	RouteEpoch uint64             `json:"-"`
	Items      []PrimitiveRequest `json:"items"`
}

type BatchPrimitiveResponse struct {
	Results []PrimitiveResult `json:"results"`
}
