package engine

import (
	"sort"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func addShardStats(stats *Stats, shard ShardStats) {
	stats.Objects += shard.Objects
	stats.MemoryBytes += shard.MemoryBytes
	stats.Evictions += shard.Evictions
	stats.GetRequests += shard.GetRequests
	stats.GetHits += shard.GetHits
	stats.GetMisses += shard.GetMisses
	stats.GetExpired += shard.GetExpired
	stats.TouchRequests += shard.TouchRequests
	stats.TouchHits += shard.TouchHits
	stats.TouchMisses += shard.TouchMisses
}

func addSpaceStats(total, shard *collectionmapping.Map[spaceKey, spaceUsage]) {
	shard.Range(func(key spaceKey, usage spaceUsage) bool {
		next, _ := total.Get(key)
		next.objects += usage.objects
		next.memoryBytes += usage.memoryBytes
		total.Set(key, next)
		return true
	})
}

func buildSpaceStats(spaces *collectionmapping.Map[spaceKey, spaceUsage]) []SpaceStats {
	out := make([]SpaceStats, 0, spaces.Len())
	spaces.Range(func(key spaceKey, usage spaceUsage) bool {
		out = append(out, SpaceStats{
			Namespace:   key.namespace,
			Space:       key.space,
			Objects:     usage.objects,
			MemoryBytes: usage.memoryBytes,
		})
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].Space < out[j].Space
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}
