package cache

import "github.com/lyonbrown4d/nespa/cache/engine"

type PrimitiveKind = engine.PrimitiveKind
type PrimitiveOptions = engine.PrimitiveOptions
type PrimitiveRequest = engine.PrimitiveRequest
type PrimitiveResult = engine.PrimitiveResult
type MapField = engine.MapField
type ScoredMember = engine.ScoredMember

const (
	PrimitiveCounterAdjust   = engine.PrimitiveCounterAdjust
	PrimitiveMapSet          = engine.PrimitiveMapSet
	PrimitiveMapGet          = engine.PrimitiveMapGet
	PrimitiveMapDelete       = engine.PrimitiveMapDelete
	PrimitiveMapGetAll       = engine.PrimitiveMapGetAll
	PrimitiveSetAdd          = engine.PrimitiveSetAdd
	PrimitiveSetRemove       = engine.PrimitiveSetRemove
	PrimitiveSetContains     = engine.PrimitiveSetContains
	PrimitiveSetMembers      = engine.PrimitiveSetMembers
	PrimitiveScoredSetPut    = engine.PrimitiveScoredSetPut
	PrimitiveScoredSetRemove = engine.PrimitiveScoredSetRemove
	PrimitiveScoredSetRange  = engine.PrimitiveScoredSetRange
)
