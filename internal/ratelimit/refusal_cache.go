package ratelimit

import (
	"hash/maphash"
	"math"
	"sync"
	"time"
)

// The refusal cache is hand-rolled, although github.com/maypok86/otter/v2 is in the module graph. Per-entry expiry in
// otter needs an ExpiryCalculator, and that starts a background goroutine per cache which ticks once a second
// (otter v2.3.0, cache_impl.go:193-195). The inline Executor option does not remove that ticker, so otter would keep a
// timer per cache. Its eviction is also policy-driven across the whole cache, while this design skips an insert into a
// full shard and sweeps the ended refusals of that shard. The sweep below is the only maintenance, and it runs inside
// an insert.

const (
	// refusalCacheCapacity is the number of refusals a node keeps for redis_user_command limits, in all shards together.
	// Each shard keeps refusalCacheCapacity / refusalShardCount refusals, rounded down, so the effective capacity is
	// 99968 refusals. The README states 100000 as the upper bound.
	refusalCacheCapacity = 100000
	// refusalShardCount is the number of independent shards of the refusal cache. Must be a power of two.
	refusalShardCount = 64
	// refusalSweepEntries is the most entries an insert into a full shard looks at for ended refusals.
	refusalSweepEntries = 16
)

// refusalSeed hashes keys to shards of the refusal cache.
var refusalSeed = maphash.MakeSeed()

// refusalShard is a part of the refusal cache guarded by its own mutex.
type refusalShard struct {
	mu sync.Mutex
	// ends holds the end of each cached refusal in unix nanoseconds, by its Redis key.
	ends map[string]int64
}

// refusalCache remembers the refusals of redis_user_command limits on this node. A refusal says that the limit of its
// key refuses until its end. Other nodes can only consume the same budget, which can lengthen a wait but never shorten
// it, so until the end Redis would refuse too, and a command inside it needs no Redis call. Entries are removed when
// they are looked up after their end, or swept by an insert into a full shard. Nothing runs in the background: an insert
// into a shard which is still full after the sweep is skipped, which only means no caching for that key.
type refusalCache struct {
	shards   [refusalShardCount]refusalShard
	perShard int
}

// newRefusalCache returns an empty cache of refusalCacheCapacity entries.
func newRefusalCache() *refusalCache {
	c := &refusalCache{perShard: refusalCacheCapacity / refusalShardCount}
	for i := range c.shards {
		c.shards[i].ends = make(map[string]int64)
	}
	return c
}

// shardOf returns the shard of key.
func (c *refusalCache) shardOf(key string) *refusalShard {
	return &c.shards[shardIndex(refusalSeed, key, refusalShardCount)]
}

// refused returns the time left of a cached refusal of key at now, and false when there is none. A refusal which
// has ended is removed.
func (c *refusalCache) refused(key string, now int64) (time.Duration, bool) {
	sh := c.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	end, ok := sh.ends[key]
	if !ok {
		return 0, false
	}
	if end <= now {
		delete(sh.ends, key)
		return 0, false
	}
	return time.Duration(end - now), true
}

// refuse caches a refusal of key which lasts wait from now. Nothing is cached for a wait of zero or less. An insert into
// a full shard first sweeps some ended refusals of it, and is skipped when the shard is still full.
func (c *refusalCache) refuse(key string, now int64, wait time.Duration) {
	if wait <= 0 {
		return
	}
	end := now + int64(wait)
	if end < now {
		// The end overflows for a wait near the largest duration, so the refusal lasts as long as the clock can count.
		end = math.MaxInt64
	}
	sh := c.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if current, ok := sh.ends[key]; ok {
		if end > current {
			sh.ends[key] = end
		}
		return
	}
	if len(sh.ends) >= c.perShard {
		sh.sweep(now)
		if len(sh.ends) >= c.perShard {
			return
		}
	}
	sh.ends[key] = end
}

// sweep removes the ended refusals among at most refusalSweepEntries entries of the shard. Map iteration starts at a
// random entry, so repeated sweeps reach all entries. The caller must hold mu.
func (sh *refusalShard) sweep(now int64) {
	visited := 0
	for key, end := range sh.ends {
		if end <= now {
			delete(sh.ends, key)
		}
		visited++
		if visited >= refusalSweepEntries {
			return
		}
	}
}
