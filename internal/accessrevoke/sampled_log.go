package accessrevoke

import (
	"sync/atomic"

	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// sampledLog limits a warning to one line per skipLogInterval. It counts the warnings it suppresses, and the next line
// it logs carries the count, so an operator sees how many were hidden.
type sampledLog struct {
	limiter    *tools.IntervalLimiter
	suppressed atomic.Int64
}

// newSampledLog returns a sampledLog which logs at most one warning per skipLogInterval.
func newSampledLog() *sampledLog {
	return &sampledLog{
		limiter: tools.NewIntervalLimiter(skipLogInterval),
	}
}

// event returns the warning to fill in when it may be logged now, with the count of the warnings suppressed since the
// last one logged. It returns nil when the warning is suppressed, and counts it.
func (l *sampledLog) event() *zerolog.Event {
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

// newSkipLogs returns a sampled log for each skip reason.
func newSkipLogs() map[string]*sampledLog {
	reasons := []string{skipUnknownMethod, skipDecodeError, skipValidation, skipDisabled, skipStale, skipApplyError}
	logs := make(map[string]*sampledLog, len(reasons))
	for _, reason := range reasons {
		logs[reason] = newSampledLog()
	}
	return logs
}
