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
	_ "embed"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
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

// tokenBucket is the token bucket algorithm implemented as GCRA (generic cell rate algorithm). Rate operations are
// allowed at once (burst), then one operation is refilled every emission interval. It is the default algorithm.
type tokenBucket struct{}

// tokenBucketSource is the Lua script of the token bucket, see scripts/token_bucket.lua.
//
//go:embed scripts/token_bucket.lua
var tokenBucketSource string

// tokenBucketScript is the Redis script of the token bucket.
var tokenBucketScript = rueidis.NewLuaScript(tokenBucketSource)

// Name returns the configuration value of the token bucket.
func (tokenBucket) Name() string {
	return "token_bucket"
}

// stateWords returns one: the TAT.
func (tokenBucket) stateWords() int {
	return 1
}

// newLimit compiles a token bucket: emission is interval / rate, tolerance is emission * rate.
func (tokenBucket) newLimit(interval time.Duration, rate int) limit {
	emission := int64(interval) / int64(rate)
	return limit{emission: emission, tolerance: emission * int64(rate)}
}

// wait returns the time until the operation is allowed, 0 when it is allowed now.
func (tokenBucket) wait(lim *limit, st []int64, now int64) int64 {
	_, wait := gcra(lim.emission, lim.tolerance, st[0], now)
	return wait
}

// take records one operation: the TAT moves by one emission interval.
func (tokenBucket) take(lim *limit, st []int64, now int64) {
	st[0], _ = gcra(lim.emission, lim.tolerance, st[0], now)
}

// redisScript returns the Redis script of the token bucket.
func (tokenBucket) redisScript() *rueidis.Lua {
	return tokenBucketScript
}

// redisArgs returns the ARGV of the token bucket script: TTL in milliseconds, number of buckets, then emission and
// burst tolerance of every bucket in microseconds. Microsecond resolution is used because Redis TIME returns
// microseconds; sub-microsecond parts of emission intervals are truncated, but never below one microsecond.
func (tokenBucket) redisArgs(lims []limit, maxInterval time.Duration) []string {
	args := make([]string, 0, 2+2*len(lims))
	args = append(args, formatInt(ttlMillis(maxInterval)), formatInt(int64(len(lims))))
	for i := range lims {
		emission := max(lims[i].emission/int64(time.Microsecond), 1)
		tolerance := max(lims[i].tolerance/int64(time.Microsecond), emission)
		args = append(args, formatInt(emission), formatInt(tolerance))
	}
	return args
}

// redisSuffix returns no suffix: the token bucket keeps the plain scope key.
func (tokenBucket) redisSuffix(_ []limit) string {
	return ""
}

// gcra returns the new TAT after one operation at now, and the wait of the operation. The wait is 0 when the
// operation is allowed; then the new TAT is the state to keep. Otherwise the TAT is not changed by the caller.
func gcra(emission int64, tolerance int64, tat int64, now int64) (int64, int64) {
	if tat < now {
		tat = now
	}
	newTAT := tat + emission
	wait := newTAT - now - tolerance
	if wait < 0 {
		wait = 0
	}
	return newTAT, wait
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
