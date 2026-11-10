package tools

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SampledLog limits a log line to one per interval. It counts the lines it suppresses, and the next line it logs
// carries the count, so an operator sees how many were hidden.
type SampledLog struct {
	limiter    *IntervalLimiter
	suppressed atomic.Int64
}

// NewSampledLog returns a SampledLog which logs at most one line per interval.
func NewSampledLog(interval time.Duration) *SampledLog {
	return &SampledLog{
		limiter: NewIntervalLimiter(interval),
	}
}

// Event returns the warning to fill in when it may be logged now, with the count of the lines suppressed since the
// last one logged. It returns nil when the warning is suppressed, and counts it.
func (l *SampledLog) Event() *zerolog.Event {
	return l.eventAt(zerolog.WarnLevel)
}

// ErrorEvent is Event at the error level, for a line which reports a fault an operator must act on.
func (l *SampledLog) ErrorEvent() *zerolog.Event {
	return l.eventAt(zerolog.ErrorLevel)
}

// eventAt returns the line of level to fill in when it may be logged now, and nil when it is suppressed.
func (l *SampledLog) eventAt(level zerolog.Level) *zerolog.Event {
	if !l.limiter.Allow() {
		l.suppressed.Add(1)
		return nil
	}
	event := log.WithLevel(level)
	if suppressed := l.suppressed.Swap(0); suppressed > 0 {
		event = event.Int64("suppressed", suppressed)
	}
	return event
}
