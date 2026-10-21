package ratelimit

import (
	_ "embed"
	"time"

	"github.com/redis/rueidis"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// fixedWindow is the fixed window counter: rate operations are allowed per window of the wall clock, and the count
// of a window is dropped when the next window starts. Its state is the window index and the count of that window.
type fixedWindow struct{}

// fixedWindowSource is the Lua body of the fixed window script, see scripts/fixed_window.lua.
//
//go:embed scripts/fixed_window.lua
var fixedWindowSource string

// fixedWindowScript is the Redis script of the fixed window, with the shared Lua prelude.
var fixedWindowScript = rueidis.NewLuaScript(luaScript(2, fixedWindowSource))

// Name returns the configuration value of the fixed window.
func (fixedWindow) Name() string {
	return configtypes.RateLimitAlgorithmFixedWindow
}

// stateWords returns two: the window index and its count.
func (fixedWindow) stateWords() int {
	return 2
}

// newLimit compiles a fixed window bucket.
func (fixedWindow) newLimit(interval time.Duration, rate int) limit {
	return windowLimit(interval, rate)
}

// wait returns 0 when the window has room, else the time until the next window starts.
func (fixedWindow) wait(lim *limit, st []int64, now int64) int64 {
	idx, e := windowAt(st[0], windowClock(now), lim.interval)
	if fixedCount(st, idx) < lim.rate {
		return 0
	}
	return lim.interval - e
}

// take counts one operation in the window of now.
func (fixedWindow) take(lim *limit, st []int64, now int64) {
	idx, _ := windowAt(st[0], windowClock(now), lim.interval)
	st[1] = fixedCount(st, idx) + 1
	st[0] = idx
}

// redisScript returns the Redis script of the fixed window.
func (fixedWindow) redisScript() *rueidis.Lua {
	return fixedWindowScript
}

// redisArgs returns the ARGV of the fixed window script. The state of a window matters until the window ends, so the
// key lives for the longest interval.
func (fixedWindow) redisArgs(lims []limit, maxInterval time.Duration) []string {
	return windowArgs(lims, maxInterval)
}

// redisSuffix names the key with the short name fw, the interval and the rate of every bucket.
func (fixedWindow) redisSuffix(lims []limit) string {
	return windowSuffix("fw", lims)
}

// fixedCount returns the count of window idx in st. A state of another window counts zero.
func fixedCount(st []int64, idx int64) int64 {
	if st[0] == idx {
		return st[1]
	}
	return 0
}
