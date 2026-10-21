package ratelimit

import (
	"strconv"
	"strings"
	"time"
)

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
