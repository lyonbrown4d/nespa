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

var primitiveValidators = map[PrimitiveKind]func(PrimitiveRequest) error{
	PrimitiveCounterAdjust:   noPrimitiveShapeValidation,
	PrimitiveMapSet:          requirePrimitiveField,
	PrimitiveMapGet:          requirePrimitiveField,
	PrimitiveMapDelete:       requirePrimitiveField,
	PrimitiveMapGetAll:       noPrimitiveShapeValidation,
	PrimitiveSetAdd:          requirePrimitiveMember,
	PrimitiveSetRemove:       requirePrimitiveMember,
	PrimitiveSetContains:     requirePrimitiveMember,
	PrimitiveSetMembers:      noPrimitiveShapeValidation,
	PrimitiveScoredSetPut:    validateScoredSetPut,
	PrimitiveScoredSetRemove: requirePrimitiveMember,
	PrimitiveScoredSetRange:  validateScoredSetRange,
	PrimitiveListPushFront:   noPrimitiveShapeValidation,
	PrimitiveListPushBack:    noPrimitiveShapeValidation,
	PrimitiveListPopFront:    noPrimitiveShapeValidation,
	PrimitiveListPopBack:     noPrimitiveShapeValidation,
	PrimitiveListRange:       validateListRange,
	PrimitiveBitmapSetBit:    validateBitmapSetBit,
	PrimitiveBitmapGetBit:    validateBitmapOffset,
	PrimitiveBitmapBitCount:  validateBitmapBitCount,
	PrimitiveHLLAdd:          requirePrimitiveMember,
	PrimitiveHLLCount:        noPrimitiveShapeValidation,
	PrimitiveHLLMerge:        noPrimitiveShapeValidation,
	PrimitiveHLLMembers:      noPrimitiveShapeValidation,
	PrimitiveGeoAdd:          validateGeoAdd,
	PrimitiveGeoDist:         validateGeoDist,
	PrimitiveGeoRadius:       validateGeoRadius,
}

func validatePrimitiveShape(request PrimitiveRequest) error {
	validator, ok := primitiveValidators[request.Kind]
	if !ok {
		return primitiveValidationError(request.Kind, "unknown kind")
	}
	return validator(request)
}

func noPrimitiveShapeValidation(PrimitiveRequest) error {
	return nil
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

func validateListRange(request PrimitiveRequest) error {
	if request.Start < 0 {
		return primitiveValidationError(request.Kind, "start must be non-negative")
	}
	return nil
}

func validateBitmapSetBit(request PrimitiveRequest) error {
	if err := validateBitmapOffset(request); err != nil {
		return err
	}
	if request.InitialValue != 0 && request.InitialValue != 1 {
		return primitiveValidationError(request.Kind, "bit value must be 0 or 1")
	}
	return nil
}

func validateBitmapOffset(request PrimitiveRequest) error {
	if request.Delta < 0 {
		return primitiveValidationError(request.Kind, "offset must be non-negative")
	}
	if int64(int(request.Delta)) != request.Delta {
		return primitiveValidationError(request.Kind, "offset is too large")
	}
	return nil
}

func validateBitmapBitCount(request PrimitiveRequest) error {
	if request.Start < 0 {
		return primitiveValidationError(request.Kind, "start must be non-negative")
	}
	if request.Limit > uint64(int(^uint(0)>>1)) {
		return primitiveValidationError(request.Kind, "limit is too large")
	}
	return nil
}

func validateGeoAdd(request PrimitiveRequest) error {
	if err := requirePrimitiveMember(request); err != nil {
		return err
	}
	if !validLongitude(request.Score) || !validLatitude(request.MinScore) {
		return primitiveValidationError(request.Kind, "invalid longitude or latitude")
	}
	return nil
}

func validateGeoDist(request PrimitiveRequest) error {
	if request.Member == "" || request.Field == "" {
		return primitiveValidationError(request.Kind, "members are required")
	}
	return nil
}

func validateGeoRadius(request PrimitiveRequest) error {
	if !validLongitude(request.Score) || !validLatitude(request.MinScore) {
		return primitiveValidationError(request.Kind, "invalid longitude or latitude")
	}
	if request.MaxScore < 0 {
		return primitiveValidationError(request.Kind, "radius must be non-negative")
	}
	return nil
}

func validLongitude(value float64) bool {
	return value >= -180 && value <= 180
}

func validLatitude(value float64) bool {
	return value >= -90 && value <= 90
}

func primitiveValidationError(kind PrimitiveKind, reason string) error {
	return oops.Code("invalid_primitive").
		In("cache.engine").
		With("kind", kind, "reason", reason).
		Wrap(ErrInvalidPrimitive)
}
