package ratelimit

import (
	"strconv"
	"time"
)

// redisScriptArgs returns arguments of the rate limit script for a scope with buckets. The arguments
// are: key TTL in milliseconds (the longest interval of the scope, rounded up), number of buckets,
// then emission interval and burst tolerance of every bucket in microseconds, the clock of the script.
// Microsecond resolution is used because Redis TIME returns microseconds; sub-microsecond parts of
// emission intervals are truncated, but never below one microsecond.
func redisScriptArgs(buckets []slotBucket, maxInterval time.Duration) []string {
	ttlMs := int64((maxInterval + time.Millisecond - 1) / time.Millisecond)
	if ttlMs < 1 {
		ttlMs = 1
	}
	args := make([]string, 0, 2+2*len(buckets))
	args = append(args, strconv.FormatInt(ttlMs, 10), strconv.Itoa(len(buckets)))
	for i := range buckets {
		emission := int64(buckets[i].bucket.emission / int64(time.Microsecond))
		if emission < 1 {
			emission = 1
		}
		tolerance := int64(buckets[i].bucket.tolerance / int64(time.Microsecond))
		if tolerance < emission {
			tolerance = emission
		}
		args = append(args, strconv.FormatInt(emission, 10), strconv.FormatInt(tolerance, 10))
	}
	return args
}
