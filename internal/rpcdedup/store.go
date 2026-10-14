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
// result evicted. Abandoned marks a call which did not complete, so nothing is kept and a duplicate
// which waited for it must retry.
type Record struct {
	Reply     []byte
	Err       *centrifuge.Error
	Uncached  bool
	Abandoned bool
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
)

// Lookup is the result of Store.Begin.
type Lookup struct {
	Outcome Outcome
	Record  Record
	Flight  *Flight
}

// Store keeps the records of the calls which exactly-once RPC tracks. The node-local store is Memory.
// A store with a shared tier is used the same way: Begin decides whether a call runs, Complete keeps
// its answer, Cancel and Ack act on the records of a msg_id.
type Store interface {
	// Begin looks key up. On a miss, flight is registered for key, and the caller owns it and must
	// call Complete.
	Begin(key Key, flight *Flight) (Lookup, error)
	// Complete keeps the record of the call which flight owns, and wakes the duplicates waiting on it.
	// An abandoned record is not kept.
	Complete(key Key, flight *Flight, rec Record) error
	// Cancel cancels the call of key when it is in flight. Otherwise it keeps key as cancelled with
	// failure, so a later call with the same msg_id is answered with it. A done key is left as it is.
	Cancel(key Key, failure *centrifuge.Error) error
	// Ack forgets the done records of the msg_ids of scope. In-flight calls are not affected.
	Ack(scope string, msgIDs []uint64) error
	// Len returns the number of records kept.
	Len() int
}
