package rpcdedup

import (
	"github.com/centrifugal/centrifuge"
)

// Key identifies a call in a store: the scope the call runs in, and its msg_id.
type Key struct {
	Scope string
	MsgID uint64
}

// Record is the answer of a call which is done. Reply is the reply payload, and Err the protocol error
// when the call failed. Uncached marks a reply over the size limit, which is not kept: a duplicate gets
// result evicted. A reply over the size limit, or over the share of a record of its scope, is kept as uncached too.
//
// Abandoned marks a flight which left no answer and kept no record. A call which runs here never abandons its
// flight: it keeps an error record when it panics. The shared tier does: its watch abandons the flight of a call
// on another node whose record vanishes or whose polls fail, and a BeginMismatch of the call which began here
// abandons the flight which local duplicates joined while the shared begin ran. A duplicate of an abandoned flight gets no retry signal, since the
// call may have run, unless NotRun is set.
type Record struct {
	Reply     []byte
	Err       *centrifuge.Error
	Uncached  bool
	Abandoned bool
	// NotRun marks an abandoned call which did not run: the shared tier failed to begin it under fail_closed, so no
	// backend call was made. Its duplicates are answered with the later retry signal, not the no-signal error.
	NotRun bool
	// sharedUncached marks an uncached record which the shared tier keeps uncached too, so its reply is not in Redis, and
	// a duplicate on this node does not poll Redis for it. It is local state, not on the wire.
	sharedUncached bool
	// tombstone marks the record of a cancelled call, which has no call and so no fingerprint. An ack keeps it, as the
	// shared tier does. It is local state, not on the wire.
	tombstone bool
	// Fingerprint is the fingerprint of the call which the record answers. Zero for a cancelled record,
	// which has no call, and answers any duplicate.
	Fingerprint [32]byte
}

// Outcome is the result of Store.Begin.
type Outcome uint8

const (
	// OutcomeMiss means no record exists, and the caller owns the flight it registered.
	OutcomeMiss Outcome = iota
	// OutcomeDone means the call is done, and Lookup.Record is its answer.
	OutcomeDone
	// OutcomeInFlight means the call runs on this node, and Lookup.Flight is its flight.
	OutcomeInFlight
	// OutcomeMismatch means a call with the same key and another fingerprint exists. The caller gets
	// neither its answer nor its flight.
	OutcomeMismatch
	// OutcomeInFlightRemote means the call runs on another node, and Lookup.Flight is the local flight which
	// is completed when the call is done. Only a Store with a shared tier returns it.
	OutcomeInFlightRemote
)

// Lookup is the result of Store.Begin.
type Lookup struct {
	Outcome Outcome
	Record  Record
	Flight  *Flight
}

// Refusal is the reason a store does not admit a scope.
type Refusal string

const (
	// RefusalPool means the pool of the scope is at its scope limit.
	RefusalPool Refusal = "pool"
	// RefusalUser means the user of the scope is at its scope limit.
	RefusalUser Refusal = "user"
)

// RefusedError is returned by Begin and Cancel when the store does not admit the scope of the key. Nothing is
// recorded for the key, and the caller decides what the call does.
type RefusedError struct {
	Refusal Refusal
}

// Error implements error.
func (e *RefusedError) Error() string {
	return "exactly-once scope not admitted: " + string(e.Refusal)
}

// PoolStats are the gauges of one pool of the store.
type PoolStats struct {
	Entries int64
	Bytes   int64
	Scopes  int64
}

// Stats are the gauges of the store. Entries and Bytes are the totals of both pools.
type Stats struct {
	Entries       int64
	Bytes         int64
	Authenticated PoolStats
	Anonymous     PoolStats
}

// Store keeps the records of the calls which exactly-once RPC tracks. The node-local store is Memory.
// A store with a shared tier is used the same way: Begin decides whether a call runs, Complete keeps
// its answer, Cancel and Ack act on the records of a msg_id.
type Store interface {
	// Begin looks key up. On a miss, flight is registered for key, and the caller owns it and must
	// call Complete. A key of a scope which the store does not admit returns a *RefusedError.
	Begin(key Key, flight *Flight) (Lookup, error)
	// Complete keeps the record of the call which flight owns, and wakes the duplicates waiting on it.
	// An abandoned record is not kept.
	Complete(key Key, flight *Flight, rec Record) error
	// Cancel cancels the call of key when it is in flight. Otherwise it keeps key as cancelled with
	// failure, so a later call with the same msg_id is answered with it. A done key is left as it is.
	// A key of a scope which the store does not admit returns a *RefusedError and keeps nothing.
	Cancel(key Key, failure *centrifuge.Error) error
	// Ack forgets the done records of the msg_ids of scope. In-flight calls are not affected.
	Ack(scope string, msgIDs []uint64) error
	// Stats returns the gauges of the store, without taking its locks.
	Stats() Stats
}
