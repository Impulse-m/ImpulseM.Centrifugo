package ratelimit

import (
	_ "embed"
	"strconv"
	"time"

	"github.com/redis/rueidis"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// tokenBucket is the token bucket algorithm implemented as GCRA (generic cell rate algorithm). Rate operations are
// allowed at once (burst), then one operation is refilled every emission interval. It is the default algorithm.
type tokenBucket struct{}

// tokenBucketSource is the Lua script of the token bucket, see scripts/token_bucket.lua.
//
//go:embed scripts/token_bucket.lua
var tokenBucketSource string

// tokenBucketScript is the Redis script of the token bucket.
var tokenBucketScript = rueidis.NewLuaScript(luaScript(1, tokenBucketSource))

// Name returns the configuration value of the token bucket.
func (tokenBucket) Name() string {
	return configtypes.RateLimitAlgorithmTokenBucket
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

// wait returns the time until the operation is allowed, 0 when it is allowed now. The client limiter does not call
// it: token-only scopes call gcra directly. It is the Algorithm implementation which the golden and Redis oracle
// tests run.
func (tokenBucket) wait(lim *limit, st []int64, now int64) int64 {
	_, wait := gcra(lim.emission, lim.tolerance, st[0], now)
	return wait
}

// take records one operation: the TAT moves by one emission interval. Like wait, it is the Algorithm implementation
// for the tests, see wait.
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
	args = append(args, strconv.FormatInt(ttlMillis(maxInterval), 10), strconv.Itoa(len(lims)))
	for i := range lims {
		emission := max(lims[i].emission/int64(time.Microsecond), 1)
		tolerance := max(lims[i].tolerance/int64(time.Microsecond), emission)
		args = append(args, strconv.FormatInt(emission, 10), strconv.FormatInt(tolerance, 10))
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
