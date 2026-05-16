package engine

import (
	"math"

	"github.com/samber/oops"
)

func validatePrimitiveRequest(request PrimitiveRequest) error {
	if err := validateKey(request.Key); err != nil {
		return err
	}
	if request.Options.TTL < 0 {
		return primitiveValidationError(request.Kind, "negative ttl")
	}
	return validatePrimitiveShape(request)
}

func validatePrimitiveShape(request PrimitiveRequest) error {
	switch request.Kind {
	case PrimitiveCounterAdjust:
		return nil
	case PrimitiveMapSet, PrimitiveMapGet, PrimitiveMapDelete:
		return requirePrimitiveField(request)
	case PrimitiveMapGetAll:
		return nil
	case PrimitiveSetAdd, PrimitiveSetRemove, PrimitiveSetContains:
		return requirePrimitiveMember(request)
	case PrimitiveSetMembers:
		return nil
	case PrimitiveScoredSetPut:
		return validateScoredSetPut(request)
	case PrimitiveScoredSetRemove:
		return requirePrimitiveMember(request)
	case PrimitiveScoredSetRange:
		return validateScoredSetRange(request)
	}
	return primitiveValidationError(request.Kind, "unknown kind")
}

func requirePrimitiveField(request PrimitiveRequest) error {
	if request.Field == "" {
		return primitiveValidationError(request.Kind, "field is required")
	}
	return nil
}

func requirePrimitiveMember(request PrimitiveRequest) error {
	if request.Member == "" {
		return primitiveValidationError(request.Kind, "member is required")
	}
	return nil
}

func validateScoredSetPut(request PrimitiveRequest) error {
	if err := requirePrimitiveMember(request); err != nil {
		return err
	}
	if invalidFloat(request.Score) {
		return primitiveValidationError(request.Kind, "score must be finite")
	}
	return nil
}

func validateScoredSetRange(request PrimitiveRequest) error {
	if request.HasMinScore && invalidFloat(request.MinScore) {
		return primitiveValidationError(request.Kind, "min score must be finite")
	}
	if request.HasMaxScore && invalidFloat(request.MaxScore) {
		return primitiveValidationError(request.Kind, "max score must be finite")
	}
	if request.HasMinScore && request.HasMaxScore && request.MinScore > request.MaxScore {
		return primitiveValidationError(request.Kind, "min score exceeds max score")
	}
	return nil
}

func invalidFloat(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}

func primitiveValidationError(kind PrimitiveKind, reason string) error {
	return oops.Code("invalid_primitive").
		In("cache.engine").
		With("kind", kind, "reason", reason).
		Wrap(ErrInvalidPrimitive)
}
