package ratelimit

import (
	"sync/atomic"
	"time"
)

// redisBreakerPause is how long the checks of a Redis shard make no Redis call after one of its calls failed. A stalled
// shard then costs one wait of the timeout per pause, instead of one per command, and a shard which recovers is used
// again within a second.
const redisBreakerPause = time.Second

// redisShardBreaker stops the checks of one Redis shard from waiting on it while it fails. It is two atomics, so a
// check takes no lock and allocates nothing. See enter and settle.
type redisShardBreaker struct {
	// openUntil is the refusalClock time until which checks make no Redis call. Zero while the shard answers.
	openUntil atomic.Int64
	// probing is set while one check tries the shard again after the pause.
	probing atomic.Bool
}

// enter reports whether a check at now may call Redis, and whether that call is the probe which tries the shard again
// after the pause. A check which may not call Redis must return the error result of the breaker, see check.
func (b *redisShardBreaker) enter(now int64) (call bool, probe bool) {
	until := b.openUntil.Load()
	if until == 0 {
		return true, false
	}
	if now < until {
		return false, false
	}
	if b.probing.CompareAndSwap(false, true) {
		return true, true
	}
	return false, false
}

// settle records the result of a Redis call which enter let through, read at now when the call returned. A failure
// opens the breaker for redisBreakerPause from now, a success closes it. A success writes nothing when the breaker is
// closed already, so the common path does not write a shared word. The probe is released last, so the next check sees
// the new state.
func (b *redisShardBreaker) settle(now int64, probe bool, failed bool) {
	if failed {
		b.openUntil.Store(now + int64(redisBreakerPause))
	} else if b.openUntil.Load() != 0 {
		b.openUntil.Store(0)
	}
	if probe {
		b.probing.Store(false)
	}
}
