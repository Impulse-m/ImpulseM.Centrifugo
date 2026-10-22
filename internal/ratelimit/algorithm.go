package ratelimit

import (
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// Algorithm is a rate limit algorithm. One Algorithm decides all buckets of a scope, in memory and in Redis.
// Adding an algorithm: one file with its type and methods, one embedded Lua script, one entry in algorithms, and its
// name in configtypes: a constant, an entry in RateLimitAlgorithmWindows, which validation reads, and one in
// RateLimitAlgorithmNames, which validation messages list. TestAlgorithmNamesMatchConfig checks both sides.
type Algorithm interface {
	// Name is the configuration value of the algorithm.
	Name() string
	// stateWords is the number of int64 words one bucket keeps in memory, and of Redis hash fields per bucket.
	stateWords() int
	// newLimit compiles one bucket. It runs once, at configuration load.
	newLimit(interval time.Duration, rate int) limit
	// wait returns 0 when one operation is allowed at now, else the time in nanoseconds until the next one is
	// allowed when nothing else is taken meanwhile. It does not change state. state holds stateWords() words.
	wait(lim *limit, state []int64, now int64) int64
	// take records one allowed operation at now. It is called only after wait returned 0 for the same state.
	take(lim *limit, state []int64, now int64)
	// redisScript is the Lua script of the algorithm, it runs the same decisions as wait and take.
	redisScript() *rueidis.Lua
	// redisArgs returns the ARGV of the script for the buckets of a scope. maxInterval is the longest interval of them.
	redisArgs(lims []limit, maxInterval time.Duration) []string
	// redisSuffix is appended to the Redis key of a scope. It names the algorithm and the parameters of the buckets,
	// so a changed configuration starts new state.
	redisSuffix(lims []limit) string
}

// limit is one compiled bucket. Each algorithm reads the fields it needs.
type limit struct {
	// interval is the window length in nanoseconds (window algorithms).
	interval int64
	// rate is the number of operations per interval (window algorithms).
	rate int64
	// emission is the GCRA emission interval in nanoseconds (token bucket).
	emission int64
	// tolerance is the GCRA burst tolerance in nanoseconds (token bucket).
	tolerance int64
}

// algorithms maps configuration values to algorithms. It is read at configuration load only.
var algorithms = map[string]Algorithm{
	tokenBucket{}.Name():   tokenBucket{},
	slidingWindow{}.Name(): slidingWindow{},
	fixedWindow{}.Name():   fixedWindow{},
}

// algorithmNamed returns the algorithm of a configuration value: the token bucket for an empty name. It panics on
// an unknown name. Validation runs before compilation, so a panic is an invariant failure, not a user error.
func algorithmNamed(name string) Algorithm {
	if name == "" {
		return tokenBucket{}
	}
	algo, ok := algorithms[name]
	if !ok {
		panic("ratelimit: unknown algorithm " + name)
	}
	return algo
}

// windowOrigin is the wall clock in nanoseconds at process start. Window algorithms count windows on the wall clock,
// as the Redis clock does: windowClock(now) is the wall clock of a monotonic nowNano value.
var windowOrigin = epoch.UnixNano()

// floorDiv returns a divided by b, rounded toward negative infinity. b must be positive.
func floorDiv(a int64, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// ceilDiv returns a divided by b, rounded toward positive infinity. b must be positive.
func ceilDiv(a int64, b int64) int64 {
	return -floorDiv(-a, b)
}

// ttlMillis returns d in whole milliseconds rounded up, at least one, as Redis key TTLs are given.
func ttlMillis(d time.Duration) int64 {
	ms := int64((d + time.Millisecond - 1) / time.Millisecond)
	return max(ms, 1)
}

// luaDivisions is the Lua prelude of the window scripts. Lua numbers are doubles, exact below 2^53, which holds for
// the magnitudes of the caps of window algorithms.
const luaDivisions = `
local function floorDiv(a, b)
	local q = math.floor(a / b)
	if q * b > a then
		q = q - 1
	elseif (q + 1) * b <= a then
		q = q + 1
	end
	return q
end

local function ceilDiv(a, b)
	return -floorDiv(-a, b)
end
`

// luaLoad is the Lua prelude of the rate limit scripts, for buckets of words state words. It reads the bucket count
// from ARGV, and the stored state of all buckets of the key in one HMGET. Each script is concatenated after it, and
// its body starts with the clock line local t = redis.call('TIME').
func luaLoad(words int) string {
	return fmt.Sprintf(`local n = tonumber(ARGV[2])
local fields = {}
for i = 1, %d * n do
	fields[i] = tostring(i - 1)
end
local stored = redis.call('HMGET', KEYS[1], unpack(fields))
`, words)
}

// luaCommit is the end of every rate limit script, concatenated after the source of each one. A refusal returns the
// largest wait of the refusing buckets, which the script left in worst, and writes nothing. An allowed operation
// writes the updates of its buckets, sets their expiry, and returns 1.
const luaCommit = `
if worst > 0 then
	return -worst
end
redis.call('HSET', KEYS[1], unpack(updates))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`

// luaScript composes the Redis script of an algorithm with buckets of words state words: the shared prelude, the
// source of the algorithm and the commit tail. The token bucket and the window algorithms all use it.
func luaScript(words int, source string) string {
	return luaDivisions + luaLoad(words) + source + luaCommit
}
