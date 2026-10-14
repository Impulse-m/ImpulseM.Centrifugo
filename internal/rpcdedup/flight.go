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
	done        chan struct{}
	record      Record
	fingerprint [32]byte
}

// NewFlight returns a flight which has not started, with a zero fingerprint, which matches any fingerprint.
// Calls of the connections use NewFlightWithFingerprint.
func NewFlight() *Flight {
	return &Flight{done: make(chan struct{})}
}

// NewFlightWithFingerprint returns a flight which has not started, for a call with fingerprint.
func NewFlightWithFingerprint(fingerprint [32]byte) *Flight {
	flight := NewFlight()
	flight.fingerprint = fingerprint
	return flight
}

// Fingerprint returns the fingerprint of the call of the flight.
func (f *Flight) Fingerprint() [32]byte {
	return f.fingerprint
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

// Cancelled reports whether Cancel was called.
func (f *Flight) Cancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// Done returns a channel which is closed when the owner completes the flight.
func (f *Flight) Done() <-chan struct{} {
	return f.done
}

// Record returns the record the owner completed the flight with. It is valid after Done is closed.
func (f *Flight) Record() Record {
	return f.record
}

// finish sets the record, and closes Done. The owner calls it once.
func (f *Flight) finish(rec Record) {
	f.record = rec
	close(f.done)
}
