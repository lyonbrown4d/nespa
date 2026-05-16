package nespa

import (
	"time"

	"github.com/lyonbrown4d/nespa/cachewire"
)

type PrimitiveKind = cachewire.PrimitiveKind

const (
	PrimitiveCounterAdjust   = cachewire.PrimitiveCounterAdjust
	PrimitiveMapSet          = cachewire.PrimitiveMapSet
	PrimitiveMapGet          = cachewire.PrimitiveMapGet
	PrimitiveMapDelete       = cachewire.PrimitiveMapDelete
	PrimitiveMapGetAll       = cachewire.PrimitiveMapGetAll
	PrimitiveSetAdd          = cachewire.PrimitiveSetAdd
	PrimitiveSetRemove       = cachewire.PrimitiveSetRemove
	PrimitiveSetContains     = cachewire.PrimitiveSetContains
	PrimitiveSetMembers      = cachewire.PrimitiveSetMembers
	PrimitiveScoredSetPut    = cachewire.PrimitiveScoredSetPut
	PrimitiveScoredSetRemove = cachewire.PrimitiveScoredSetRemove
	PrimitiveScoredSetRange  = cachewire.PrimitiveScoredSetRange
	PrimitiveListPushFront   = cachewire.PrimitiveListPushFront
	PrimitiveListPushBack    = cachewire.PrimitiveListPushBack
	PrimitiveListPopFront    = cachewire.PrimitiveListPopFront
	PrimitiveListPopBack     = cachewire.PrimitiveListPopBack
	PrimitiveListRange       = cachewire.PrimitiveListRange
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

type MapField struct {
	Field string
	Value []byte
}

type ScoredMember struct {
	Member string
	Score  float64
}
