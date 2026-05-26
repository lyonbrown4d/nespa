package engine

import (
	"bytes"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/samber/oops"
)

type collectionKind byte

const (
	collectionKindMap collectionKind = iota + 1
	collectionKindSet
	collectionKindScoredSet
	collectionKindList
	collectionKindBitmap
	collectionKindHLL
	collectionKindGeo
)

var collectionValuePrefix = []byte{'n', 's', 'p', 'a', 'c', '1'}

func encodeMapCollection(fields *collectionmapping.Map[string, []byte]) ([]byte, error) {
	body, err := fields.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode map collection", err)
	}
	return encodeCollectionBody(collectionKindMap, body), nil
}

func decodeMapCollection(raw []byte) (*collectionmapping.Map[string, []byte], error) {
	body, err := decodeCollectionBody(raw, collectionKindMap)
	if err != nil {
		return nil, err
	}
	fields := collectionmapping.NewMap[string, []byte]()
	if err := fields.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode map collection", err)
	}
	return fields, nil
}

func encodeSetCollection(members *collectionset.Set[string]) ([]byte, error) {
	body, err := members.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode set collection", err)
	}
	return encodeCollectionBody(collectionKindSet, body), nil
}

func decodeSetCollection(raw []byte) (*collectionset.Set[string], error) {
	body, err := decodeCollectionBody(raw, collectionKindSet)
	if err != nil {
		return nil, err
	}
	members := collectionset.NewSet[string]()
	if err := members.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode set collection", err)
	}
	return members, nil
}

func encodeScoredSetCollection(scores *collectionmapping.Map[string, float64]) ([]byte, error) {
	body, err := scores.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode scored set collection", err)
	}
	return encodeCollectionBody(collectionKindScoredSet, body), nil
}

func decodeScoredSetCollection(raw []byte) (*collectionmapping.Map[string, float64], error) {
	body, err := decodeCollectionBody(raw, collectionKindScoredSet)
	if err != nil {
		return nil, err
	}
	scores := collectionmapping.NewMap[string, float64]()
	if err := scores.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode scored set collection", err)
	}
	return scores, nil
}

func encodeListCollection(values *collectionlist.List[[]byte]) ([]byte, error) {
	body, err := values.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode list collection", err)
	}
	return encodeCollectionBody(collectionKindList, body), nil
}

func decodeListCollection(raw []byte) (*collectionlist.List[[]byte], error) {
	body, err := decodeCollectionBody(raw, collectionKindList)
	if err != nil {
		return nil, err
	}
	values := collectionlist.NewList[[]byte]()
	if err := values.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode list collection", err)
	}
	return values, nil
}

func encodeBitmapCollection(bits *collectionset.Set[int]) ([]byte, error) {
	body, err := bits.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode bitmap collection", err)
	}
	return encodeCollectionBody(collectionKindBitmap, body), nil
}

func decodeBitmapCollection(raw []byte) (*collectionset.Set[int], error) {
	body, err := decodeCollectionBody(raw, collectionKindBitmap)
	if err != nil {
		return nil, err
	}
	bits := collectionset.NewSet[int]()
	if err := bits.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode bitmap collection", err)
	}
	return bits, nil
}

func encodeHLLCollection(hashes *collectionset.Set[string]) ([]byte, error) {
	body, err := hashes.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode hll collection", err)
	}
	return encodeCollectionBody(collectionKindHLL, body), nil
}

func decodeHLLCollection(raw []byte) (*collectionset.Set[string], error) {
	body, err := decodeCollectionBody(raw, collectionKindHLL)
	if err != nil {
		return nil, err
	}
	hashes := collectionset.NewSet[string]()
	if err := hashes.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode hll collection", err)
	}
	return hashes, nil
}

func encodeGeoCollection(points *collectionmapping.Map[string, GeoPoint]) ([]byte, error) {
	body, err := points.MarshalBinary()
	if err != nil {
		return nil, collectionSerializationError("encode geo collection", err)
	}
	return encodeCollectionBody(collectionKindGeo, body), nil
}

func decodeGeoCollection(raw []byte) (*collectionmapping.Map[string, GeoPoint], error) {
	body, err := decodeCollectionBody(raw, collectionKindGeo)
	if err != nil {
		return nil, err
	}
	points := collectionmapping.NewMap[string, GeoPoint]()
	if err := points.UnmarshalBinary(body); err != nil {
		return nil, collectionSerializationError("decode geo collection", err)
	}
	return points, nil
}

func encodeCollectionBody(kind collectionKind, body []byte) []byte {
	raw := make([]byte, 0, len(collectionValuePrefix)+1+len(body))
	raw = append(raw, collectionValuePrefix...)
	raw = append(raw, byte(kind))
	return append(raw, body...)
}

func decodeCollectionBody(raw []byte, expected collectionKind) ([]byte, error) {
	prefixSize := len(collectionValuePrefix)
	if len(raw) <= prefixSize || !bytes.Equal(raw[:prefixSize], collectionValuePrefix) {
		return nil, invalidCollectionError("missing collection prefix")
	}
	if collectionKind(raw[prefixSize]) != expected {
		return nil, invalidCollectionError("collection kind mismatch")
	}
	return raw[prefixSize+1:], nil
}

func invalidCollectionError(reason string) error {
	return oops.Code("invalid_collection").
		In("cache.engine").
		With("reason", reason).
		Wrap(ErrInvalidCollection)
}

func collectionSerializationError(reason string, err error) error {
	return oops.Code("collection_serialization_failed").
		In("cache.engine").
		With("reason", reason, "cause", err.Error()).
		Wrap(ErrInvalidCollection)
}
