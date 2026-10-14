package rpcdedup

import (
	"context"
	"sync"
)

// InFlight is the map of the calls with a msg_id which run on one connection, by msg_id, with the
// function which cancels each of them. It is kept in the connection storage and is safe for concurrent
// use.
type InFlight struct {
	mu    sync.Mutex
	calls map[uint64]*inflightCall
}

// inflightCall is one entry of InFlight. Its pointer identifies the call, so a release does not remove
// a newer call with the same msg_id.
type inflightCall struct {
	cancel context.CancelFunc
}

// NewInFlight returns an empty map.
func NewInFlight() *InFlight {
	return &InFlight{calls: map[uint64]*inflightCall{}}
}

// Add registers the call msgID with its cancel function. The returned function removes the call.
func (f *InFlight) Add(msgID uint64, cancel context.CancelFunc) func() {
	call := &inflightCall{cancel: cancel}
	f.mu.Lock()
	f.calls[msgID] = call
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.calls[msgID] == call {
			delete(f.calls, msgID)
		}
	}
}

// Cancel cancels the call msgID when it runs on this connection. It does nothing for other msg_ids.
func (f *InFlight) Cancel(msgID uint64) {
	f.mu.Lock()
	call := f.calls[msgID]
	f.mu.Unlock()
	if call != nil {
		call.cancel()
	}
}
