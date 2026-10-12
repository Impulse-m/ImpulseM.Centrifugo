// Package ratelimit implements operation rate limits of client connections.
//
// Limits use the token bucket algorithm implemented as GCRA (generic cell rate algorithm):
// each bucket keeps a single int64 theoretical arrival time (TAT) instead of a token count,
// so checks need no floats and no allocations.
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

// Bucket is a GCRA token bucket with capacity equal to the configured rate. Rate operations
// are allowed at once (burst), then one operation is refilled every emission interval.
type Bucket struct {
	// emission is a time between two tokens: interval / rate.
	emission int64
	// tolerance is a burst allowance: emission * rate.
	tolerance int64
}

// NewBucket creates a bucket which allows rate operations per interval.
// Interval must be at least rate nanoseconds, the caller validates it.
func NewBucket(interval time.Duration, rate int) Bucket {
	emission := int64(interval) / int64(rate)
	return Bucket{
		emission:  emission,
		tolerance: emission * int64(rate),
	}
}

// next returns the new theoretical arrival time if an operation happens at now and the bucket
// state is tat. ok is false when the operation is over the limit.
func (b Bucket) next(now int64, tat int64) (int64, bool) {
	if tat < now {
		tat = now
	}
	newTAT := tat + b.emission
	if newTAT-now > b.tolerance {
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
