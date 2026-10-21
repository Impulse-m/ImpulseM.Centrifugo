// Package ratelimit implements operation rate limits of client connections.
//
// A limit is a list of buckets which all must allow an operation. Each bucket names an algorithm: token_bucket
// (GCRA, the default), sliding_window (sliding window counter) or fixed_window. The window algorithms count
// windows on the wall clock, as Redis does; in memory the window boundaries follow the wall clock as it was at
// process start, so after a wall clock step they stay where they were until restart. Redis boundaries are exact.
//
// Token bucket state is one int64 theoretical arrival time (TAT), so checks need no floats and no allocations.
// Window state is a few int64 counters per bucket, all integer arithmetic.
//
// Redis keys of a scope are {prefix}.rl.{userID}.{scope}. The token bucket keeps the plain key, the window
// algorithms add a suffix with their short name, interval and rate, so a changed configuration starts new state.
//
// To add an algorithm: implement Algorithm in a new file, embed its Lua script, and add it to algorithms.
package ratelimit

import (
	"sync/atomic"
	"time"
)

// epoch is a monotonic reference point. Times are converted to int64 nanoseconds since epoch,
// so bucket state is a plain integer which is zero (in the past) for a fresh bucket.
var epoch = time.Now()

// nowNano converts time to nanoseconds used by buckets. Zero time falls back to the current time.
func nowNano(at time.Time) int64 {
	if at.IsZero() {
		at = time.Now()
	}
	return int64(at.Sub(epoch))
}

// Bucket is a GCRA token bucket with capacity equal to the configured rate. It is used by client error limits,
// which are token buckets only.
type Bucket struct {
	// emission is a time between two tokens: interval / rate.
	emission int64
	// tolerance is a burst allowance: emission * rate.
	tolerance int64
}

// NewBucket creates a bucket which allows rate operations per interval.
// Interval must be at least rate nanoseconds, the caller validates it.
func NewBucket(interval time.Duration, rate int) Bucket {
	lim := tokenBucket{}.newLimit(interval, rate)
	return Bucket{
		emission:  lim.emission,
		tolerance: lim.tolerance,
	}
}

// next returns the new theoretical arrival time if an operation happens at now and the bucket
// state is tat. ok is false when the operation is over the limit.
func (b Bucket) next(now int64, tat int64) (int64, bool) {
	newTAT, wait := gcra(b.emission, b.tolerance, tat, now)
	if wait > 0 {
		return 0, false
	}
	return newTAT, true
}

// allowAtomic checks the bucket with state stored in tat and commits the operation if allowed.
// It is safe for concurrent use and retries on concurrent updates of tat.
func (b Bucket) allowAtomic(tat *atomic.Int64, now int64) bool {
	for {
		old := tat.Load()
		newTAT, ok := b.next(now, old)
		if !ok {
			return false
		}
		if tat.CompareAndSwap(old, newTAT) {
			return true
		}
	}
}
