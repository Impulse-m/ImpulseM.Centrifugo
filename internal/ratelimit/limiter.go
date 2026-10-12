package ratelimit

import (
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// ClientLimiter holds compiled per-connection limits of client commands and client errors.
// Its methods are safe for concurrent use when they are given different ConnState values,
// command checks must be called only from the transport reader goroutine of a connection.
type ClientLimiter struct {
	commands    *CommandPlan
	errorLimits []Bucket
}

// ConnState is a per-connection rate limit state. Command slots are touched only from the
// transport reader goroutine, error counters are atomic since errors may be processed
// concurrently.
type ConnState struct {
	slots    []int64
	errorTAT []atomic.Int64
}

// NewClientLimiter compiles client_command and client_error limits. Nothing is compiled for
// disabled sections.
func NewClientLimiter(cfg configtypes.RateLimit) *ClientLimiter {
	l := &ClientLimiter{}
	if cfg.ClientCommand.Enabled {
		l.commands = newCommandPlan(cfg.ClientCommand)
	}
	if cfg.ClientError.Enabled && cfg.ClientError.Total.Enabled {
		for _, item := range cfg.ClientError.Total.Buckets {
			l.errorLimits = append(l.errorLimits, NewBucket(item.Interval.ToDuration(), item.Rate))
		}
	}
	return l
}

// NewConnState allocates per-connection state. It is called once per connection.
func (l *ClientLimiter) NewConnState() *ConnState {
	st := &ConnState{}
	if l.commands != nil {
		st.slots = make([]int64, l.commands.numSlots)
	}
	if len(l.errorLimits) > 0 {
		st.errorTAT = make([]atomic.Int64, len(l.errorLimits))
	}
	return st
}

// NeedsKey reports whether command checks for op depend on the namespace (channel operations)
// or method (RPC). When false the key may be empty and the lookup can be skipped.
func (l *ClientLimiter) NeedsKey(op Operation) bool {
	return l.commands != nil && l.commands.needsKey(op)
}

// CommandAllowed checks and consumes one command of operation op at time at. key is a namespace
// name for channel operations and a method name for RPC. It returns false when the command is over
// the limit, in which case nothing is consumed.
func (l *ClientLimiter) CommandAllowed(st *ConnState, at time.Time, op Operation, key string) bool {
	if l.commands == nil {
		return true
	}
	return l.commands.allow(st.slots, nowNano(at), op, key)
}

// ErrorAllowed counts one protocol error of a connection. It returns false when the error limit
// is exhausted and the connection must be closed.
func (l *ClientLimiter) ErrorAllowed(st *ConnState, at time.Time) bool {
	if len(l.errorLimits) == 0 {
		return true
	}
	now := nowNano(at)
	for i := range l.errorLimits {
		if _, ok := l.errorLimits[i].next(now, st.errorTAT[i].Load()); !ok {
			return false
		}
	}
	allowed := true
	for i := range l.errorLimits {
		if !l.errorLimits[i].allowAtomic(&st.errorTAT[i], now) {
			allowed = false
		}
	}
	return allowed
}
