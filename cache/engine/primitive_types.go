package engine

import "time"

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

type PrimitiveOptions struct {
	TTL              time.Duration
	NamespaceVersion uint64
	SpaceVersion     uint64
	ExpectedVersion  uint64
}

type PrimitiveRequest struct {
	Kind         PrimitiveKind
	Key          Key
	Options      PrimitiveOptions
	Field        string
	Member       string
	Value        []byte
	Delta        int64
	InitialValue int64
	Score        float64
	MinScore     float64
	MaxScore     float64
	HasMinScore  bool
	HasMaxScore  bool
	Limit        uint64
	Start        int64
	Reverse      bool
}

type PrimitiveResult struct {
	Record        Record
	Found         bool
	Applied       bool
	Value         []byte
	Bool          bool
	Count         uint64
	Fields        []MapField
	Members       []string
	ScoredMembers []ScoredMember
	Values        [][]byte
}

type WriteEstimate struct {
	Key          Key
	Applied      bool
	OldCostBytes uint64
	NewCostBytes uint64
}

type PrimitiveEstimate = WriteEstimate

type MapField struct {
	Field string
	Value []byte
}

type ScoredMember struct {
	Member string
	Score  float64
}

type GeoPoint struct {
	Longitude float64
	Latitude  float64
}

func (k PrimitiveKind) Mutates() bool {
	switch k {
	case PrimitiveCounterAdjust,
		PrimitiveMapSet,
		PrimitiveMapDelete,
		PrimitiveSetAdd,
		PrimitiveSetRemove,
		PrimitiveScoredSetPut,
		PrimitiveScoredSetRemove,
		PrimitiveListPushFront,
		PrimitiveListPushBack,
		PrimitiveListPopFront,
		PrimitiveListPopBack,
		PrimitiveBitmapSetBit,
		PrimitiveHLLAdd,
		PrimitiveHLLMerge,
		PrimitiveGeoAdd:
		return true
	case PrimitiveMapGet,
		PrimitiveMapGetAll,
		PrimitiveSetContains,
		PrimitiveSetMembers,
		PrimitiveScoredSetRange,
		PrimitiveListRange,
		PrimitiveBitmapGetBit,
		PrimitiveBitmapBitCount,
		PrimitiveHLLCount,
		PrimitiveHLLMembers,
		PrimitiveGeoDist,
		PrimitiveGeoRadius:
		return false
	}
	return false
}
