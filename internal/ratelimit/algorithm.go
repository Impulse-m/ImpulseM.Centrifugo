package ratelimit

import (
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// Algorithm is a rate limit algorithm. One Algorithm decides all buckets of a scope, in memory and in Redis.
// Adding an algorithm: one file with its type and methods, one embedded Lua script, one entry in algorithms.
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

// windowClock converts a nowNano value to the window clock in nanoseconds.
func windowClock(now int64) int64 {
	return now + windowOrigin
}

// windowAt returns the window index of the window clock t, for windows of length w, and the offset of t in that
// window. A stored index one window ahead of t is kept, since a command with an earlier time may be ordered after a
// later one: the offset is then negative and the state is never moved back. A stored index further ahead is stale,
// the window of t is used, and the caller finds the state of other windows empty.
func windowAt(stored int64, t int64, w int64) (int64, int64) {
	idx := floorDiv(t, w)
	if stored == idx+1 {
		idx = stored
	}
	return idx, t - idx*w
}

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

// formatInt returns v in decimal, the form of Redis script arguments.
func formatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// ttlMillis returns d in whole milliseconds rounded up, at least one, as Redis key TTLs are given.
func ttlMillis(d time.Duration) int64 {
	ms := int64((d + time.Millisecond - 1) / time.Millisecond)
	return max(ms, 1)
}

// luaDivisions is the Lua prelude of the window scripts. Lua numbers are doubles, exact below 2^53, which holds for
// the magnitudes of the caps of window algorithms. It is concatenated in front of each window script.
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

// windowLimit compiles a window bucket.
func windowLimit(interval time.Duration, rate int) limit {
	return limit{interval: int64(interval), rate: int64(rate)}
}

// windowArgs returns the ARGV of a window algorithm: TTL in milliseconds, number of buckets, then the window length
// in microseconds and the rate of every bucket. The TTL is ttl rounded up to milliseconds.
func windowArgs(lims []limit, ttl time.Duration) []string {
	args := make([]string, 0, 2+2*len(lims))
	args = append(args, strconv.FormatInt(ttlMillis(ttl), 10), strconv.Itoa(len(lims)))
	for i := range lims {
		args = append(args, strconv.FormatInt(lims[i].interval/int64(time.Microsecond), 10))
		args = append(args, strconv.FormatInt(lims[i].rate, 10))
	}
	return args
}

// windowSuffix returns the Redis key suffix of a window algorithm: a short name of the algorithm, then the interval in
// milliseconds and the rate of every bucket. Intervals are whole milliseconds, see validation.
func windowSuffix(short string, lims []limit) string {
	var sb strings.Builder
	sb.WriteString("~" + short)
	for i := range lims {
		sb.WriteString("." + strconv.FormatInt(lims[i].interval/int64(time.Millisecond), 10))
		sb.WriteString("." + strconv.FormatInt(lims[i].rate, 10))
	}
	return sb.String()
}
