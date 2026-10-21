package ratelimit

import (
	_ "embed"
	"time"

	"github.com/redis/rueidis"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// slidingWindow is the sliding window counter (the rule of Cloudflare): the count of the previous window, weighted by
// the part of it still inside the sliding window, plus the count of the current window must stay below the rate.
// Its state is the window index, the count of that window and the count of the previous window.
type slidingWindow struct{}

// slidingWindowSource is the Lua body of the sliding window script, see scripts/sliding_window.lua.
//
//go:embed scripts/sliding_window.lua
var slidingWindowSource string

// slidingWindowScript is the Redis script of the sliding window, with the shared Lua prelude.
var slidingWindowScript = rueidis.NewLuaScript(luaScript(3, slidingWindowSource))

// Name returns the configuration value of the sliding window.
func (slidingWindow) Name() string {
	return configtypes.RateLimitAlgorithmSlidingWindow
}

// stateWords returns three: the window index, its count and the count of the previous window.
func (slidingWindow) stateWords() int {
	return 3
}

// newLimit compiles a sliding window bucket.
func (slidingWindow) newLimit(interval time.Duration, rate int) limit {
	return windowLimit(interval, rate)
}

// wait returns 0 when the operation is allowed now, else the time until it is allowed. The decision uses the start
// of the stored window when its index is one window ahead (see windowAt), the wait uses the real time.
func (slidingWindow) wait(lim *limit, st []int64, now int64) int64 {
	w := lim.interval
	_, e, prev, cur := slidingAt(st, windowClock(now), w)
	if prev*(w-max(e, 0)) < (lim.rate-cur)*w {
		return 0
	}
	return slidingRefusedWait(w, lim.rate, prev, cur, e)
}

// take counts one operation in the window of now, the previous count is kept.
func (slidingWindow) take(lim *limit, st []int64, now int64) {
	idx, _, prev, cur := slidingAt(st, windowClock(now), lim.interval)
	st[0] = idx
	st[1] = cur + 1
	st[2] = prev
}

// redisScript returns the Redis script of the sliding window.
func (slidingWindow) redisScript() *rueidis.Lua {
	return slidingWindowScript
}

// redisArgs returns the ARGV of the sliding window script. The previous window matters until the window after it
// ends, so the key lives for twice the longest interval.
func (slidingWindow) redisArgs(lims []limit, maxInterval time.Duration) []string {
	return windowArgs(lims, 2*maxInterval)
}

// redisSuffix names the key with the short name sw, the interval and the rate of every bucket.
func (slidingWindow) redisSuffix(lims []limit) string {
	return windowSuffix("sw", lims)
}

// slidingAt returns the window index of the window clock t, the offset e of t in that window, and the counts of the
// previous and the current window. A state of another window counts zero.
func slidingAt(st []int64, t int64, w int64) (idx int64, e int64, prev int64, cur int64) {
	idx, e = windowAt(st[0], t, w)
	switch st[0] {
	case idx:
		prev = st[2]
		cur = st[1]
	case idx - 1:
		prev = st[1]
	}
	return idx, e, prev, cur
}

// slidingRefusedWait returns the wait of a refused operation in window offset e with counts prev and cur. It is the
// first offset eStar where prev*(w-eStar) < (rate-cur)*w holds, which is exact over integers, or else the next window.
func slidingRefusedWait(w int64, rate int64, prev int64, cur int64, e int64) int64 {
	if b := rate - cur; b >= 1 && prev > 0 {
		if q := ceilDiv(b*w, prev); q >= 2 {
			return w - q + 1 - e
		}
	}
	return nextWindowWait(w, rate, cur, e)
}

// nextWindowWait returns the wait from offset e until the next window allows an operation. The next window starts with
// cur as its previous count and an empty current count, so it allows from its first offset x with
// cur*(w-x) < rate*w, that is x >= w - ceil(rate*w/cur) + 1, or from its start when cur is zero.
func nextWindowWait(w int64, rate int64, cur int64, e int64) int64 {
	if cur == 0 {
		return w - e
	}
	q := ceilDiv(rate*w, cur)
	return w - e + max(0, w-q+1)
}
