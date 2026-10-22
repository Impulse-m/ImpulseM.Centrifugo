package tools

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SampledLog limits a warning to one line per interval. It counts the warnings it suppresses, and the next line it logs
// carries the count, so an operator sees how many were hidden.
type SampledLog struct {
	limiter    *IntervalLimiter
	suppressed atomic.Int64
}

// NewSampledLog returns a SampledLog which logs at most one warning per interval.
func NewSampledLog(interval time.Duration) *SampledLog {
	return &SampledLog{
		limiter: NewIntervalLimiter(interval),
	}
}

// Event returns the warning to fill in when it may be logged now, with the count of the warnings suppressed since the
// last one logged. It returns nil when the warning is suppressed, and counts it.
func (l *SampledLog) Event() *zerolog.Event {
	if !l.limiter.Allow() {
		l.suppressed.Add(1)
		return nil
	}
	event := log.Warn()
	if suppressed := l.suppressed.Swap(0); suppressed > 0 {
		event = event.Int64("suppressed", suppressed)
	}
	return event
}
