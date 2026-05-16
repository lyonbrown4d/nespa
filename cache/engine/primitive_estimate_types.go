package engine

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type mapCollection = *collectionmapping.Map[string, []byte]
type setCollection = *collectionset.Set[string]
type scoredSetCollection = *collectionmapping.Map[string, float64]
type listCollection = *collectionlist.List[[]byte]

func unchangedPrimitiveEstimate(cmd shardCommand, ent *entry) PrimitiveEstimate {
	return PrimitiveEstimate{
		Key:          cmd.key,
		Applied:      true,
		OldCostBytes: ent.costBytes,
		NewCostBytes: ent.costBytes,
	}
}
