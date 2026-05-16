package engine

import (
	"bytes"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/samber/oops"
)

type collectionKind byte

const (
	collectionKindMap collectionKind = iota + 1
	collectionKindSet
	collectionKindScoredSet
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
