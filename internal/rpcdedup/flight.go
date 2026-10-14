package rpcdedup

import (
	"context"
	"sync"
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
}

// NewFlight returns a flight which has not started, with a zero fingerprint. A zero fingerprint matches only a
// cancelled record, and a flight with the zero fingerprint only the flight of a zero fingerprint. Calls of the
// connections use NewFlightWithFingerprint.
func NewFlight() *Flight {
	return &Flight{done: make(chan struct{})}
}

// NewFlightWithFingerprint returns a flight which has not started, for a call with fingerprint.
func NewFlightWithFingerprint(fingerprint [32]byte) *Flight {
	flight := NewFlight()
	flight.fingerprint = fingerprint
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
