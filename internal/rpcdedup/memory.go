package rpcdedup

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// zeroFingerprint is the fingerprint of a record which has no call, such as a cancelled one.
var zeroFingerprint [32]byte

// Memory is the node-local store. The done records are kept in an expirable LRU, bounded in entries and
// expiring after the window. The calls in flight on this node are kept in a map, so a duplicate on
// another connection of the node joins the call. Records are not shared between nodes.
type Memory struct {
	mu            sync.Mutex
	done          *expirable.LRU[Key, Record]
	flights       map[Key]*Flight
	maxReplyBytes int
}

// NewMemory returns an empty node-local store which keeps at most maxEntries done records for ttl. A
// reply over maxReplyBytes is kept as uncached.
func NewMemory(maxEntries int, ttl time.Duration, maxReplyBytes int) *Memory {
	done := expirable.NewLRU[Key, Record](maxEntries, nil, ttl)
	flights := map[Key]*Flight{}
	return &Memory{
		done:          done,
		flights:       flights,
		maxReplyBytes: maxReplyBytes,
	}
}

// Begin implements Store.
func (m *Memory) Begin(key Key, flight *Flight) (Lookup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rec Record
	var found bool
	rec, found = m.done.Get(key)
	if found {
		if rec.Fingerprint != zeroFingerprint && rec.Fingerprint != flight.fingerprint {
			return Lookup{Outcome: OutcomeMismatch}, nil
		}
		return Lookup{Outcome: OutcomeDone, Record: rec}, nil
	}
	var running *Flight
	running, found = m.flights[key]
	if found {
		if running.fingerprint != flight.fingerprint {
			return Lookup{Outcome: OutcomeMismatch}, nil
		}
		return Lookup{Outcome: OutcomeInFlight, Flight: running}, nil
	}
	m.flights[key] = flight
	return Lookup{Outcome: OutcomeMiss}, nil
}

// Complete implements Store. The record is kept before the flight is removed from the map and woken
// up, so a call which misses the done records always finds the flight.
func (m *Memory) Complete(key Key, flight *Flight, rec Record) error {
	rec.Fingerprint = flight.fingerprint
	m.mu.Lock()
	if m.flights[key] == flight {
		delete(m.flights, key)
	}
	if !rec.Abandoned {
		kept := rec
		if rec.Err == nil && len(rec.Reply) > m.maxReplyBytes {
			kept = Record{Uncached: true}
		}
		m.done.Add(key, kept)
	}
	m.mu.Unlock()
	flight.finish(rec)
	return nil
}

// Cancel implements Store. The flight is cancelled outside the lock.
func (m *Memory) Cancel(key Key, failure *centrifuge.Error) error {
	m.mu.Lock()
	var running *Flight
	var found bool
	running, found = m.flights[key]
	if found {
		m.mu.Unlock()
		running.Cancel()
		return nil
	}
	_, found = m.done.Peek(key)
	if !found {
		m.done.Add(key, Record{Err: failure})
	}
	m.mu.Unlock()
	return nil
}

// Ack implements Store.
func (m *Memory) Ack(scope string, msgIDs []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msgID := range msgIDs {
		key := Key{Scope: scope, MsgID: msgID}
		m.done.Remove(key)
	}
	return nil
}

// Len implements Store. Records which expired may still be counted until the next cleanup, which
// runs about every hundredth part of the window.
func (m *Memory) Len() int {
	return m.done.Len()
}
