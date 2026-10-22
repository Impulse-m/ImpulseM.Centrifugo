package ratelimit

import "hash/maphash"

// shardIndex returns the shard of key among count shards, count being a power of two. It does not allocate.
func shardIndex(seed maphash.Seed, key string, count int) int {
	return int(maphash.String(seed, key) & uint64(count-1))
}
