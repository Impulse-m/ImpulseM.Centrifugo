package rpcdedup

import (
	"context"
	"sync"
	"time"
)

// Flight is a call which runs on this node. Its owner completes it, and duplicates wait for it. Cancel
// cancels the backend call of the owner.
type Flight struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	cancelled   bool
	finished    bool
	done        chan struct{}
	record      Record
	fingerprint [32]byte
	// timeout is the method timeout of the call, zero when unknown. It bounds the in-flight record of the
	// call in a shared tier.
	timeout time.Duration
	// begun is the time in Unix nanoseconds when the in-flight record of the call was begun in a shared tier, the start
	// of its in-flight TTL, or zero when it was not. An integer keeps a flight small when the shared tier is off.
	// The owner sets it and reads it.
	begun int64
	// localOnly is set when the call runs without the shared tier, see Shared.Begin.
	localOnly bool
	// remote is set when the call runs on another node, and this flight waits for its answer.
	remote bool
	// wake is signalled when the owner of the call on another node notifies this node that the record is written. It is
	// made by markRemote when the wakeup is on, and nil otherwise, so a flight costs nothing more when the wakeup is off.
	wake chan struct{}
	// wakePending is a wakeup which came before markRemote: markRemote makes wake pre-filled then.
	wakePending bool
}

// NewFlight returns a flight which has not started, with a zero fingerprint. A zero fingerprint matches only a
// cancelled record, and a flight with the zero fingerprint only the flight of a zero fingerprint. Calls use
// NewCallFlight, which sets the method timeout.
func NewFlight() *Flight {
	return &Flight{done: make(chan struct{})}
}

// NewFlightWithFingerprint returns a flight which has not started, for a call with fingerprint.
func NewFlightWithFingerprint(fingerprint [32]byte) *Flight {
	flight := NewFlight()
	flight.fingerprint = fingerprint
	return flight
}

// NewCallFlight returns a flight which has not started, for a call with fingerprint and method timeout.
// The timeout bounds the in-flight record of the call in a shared tier.
func NewCallFlight(fingerprint [32]byte, timeout time.Duration) *Flight {
	flight := NewFlightWithFingerprint(fingerprint)
	flight.timeout = timeout
	return flight
}

// Attach sets the function which cancels the backend call of the owner. If the flight was cancelled
// before, cancel runs now.
func (f *Flight) Attach(cancel context.CancelFunc) {
	f.mu.Lock()
	f.cancel = cancel
	cancelled := f.cancelled
	f.mu.Unlock()
	if cancelled {
		cancel()
	}
}

// Cancel marks the flight as cancelled, and cancels the backend call of the owner.
func (f *Flight) Cancel() {
	f.mu.Lock()
	f.cancelled = true
	cancel := f.cancel
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// markRemote marks the flight as waiting for a call which runs on another node. With wakeup set it makes the wake
// channel, pre-filled when a wakeup came before. It reports whether the flight was cancelled before, while it had no
// call to cancel: the caller then tells the node which runs the call.
func (f *Flight) markRemote(wakeup bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remote = true
	if wakeup {
		f.wake = make(chan struct{}, 1)
		if f.wakePending {
			f.wake <- struct{}{}
		}
	}
	return f.cancelled
}

// nudge signals the watcher of the flight that the record may be written. Several nudges coalesce. A nudge before
// markRemote is kept for it, and a nudge on a flight which never turns remote is kept and never read.
func (f *Flight) nudge() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wake == nil {
		f.wakePending = true
		return
	}
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// isRemote reports whether the flight waits for a call which runs on another node.
func (f *Flight) isRemote() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remote
}

// Done returns a channel which is closed when the owner completes the flight.
func (f *Flight) Done() <-chan struct{} {
	return f.done
}

// Record returns the record the owner completed the flight with. It is valid after Done is closed.
func (f *Flight) Record() Record {
	return f.record
}

// finish sets the record, and closes Done. A flight finishes once: a later call keeps the first record.
func (f *Flight) finish(rec Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finished {
		return
	}
	f.finished = true
	f.record = rec
	close(f.done)
}
