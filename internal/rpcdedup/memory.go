package rpcdedup

import (
	"container/list"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
)

// memoryShards is the number of shards of the node-local store. A shard has its own mutex, and its share of the
// entry and byte budgets. A scope is always in one shard, so its cap is enforced in one place.
const memoryShards = 64

// recordOverhead is the size counted for a record besides its reply, so that records without a reply count too.
const recordOverhead = 128

// zeroFingerprint is the fingerprint of a record which has no call, such as a cancelled one.
var zeroFingerprint [32]byte

// MemoryOptions configures the node-local store. A zero limit is not enforced.
type MemoryOptions struct {
	// MaxEntries bounds the records kept by the node. It is shared out over the shards.
	MaxEntries int
	// MaxBytes bounds the bytes of the records kept by the node. It is shared out over the shards.
	MaxBytes int64
	// MaxRecordsPerScope bounds the records kept for one scope. The oldest record of the scope goes first.
	MaxRecordsPerScope int
	// TTL is how long a record is kept.
	TTL time.Duration
	// MaxReplyBytes is the size above which a reply is not kept.
	MaxReplyBytes int
}

// Memory is the node-local store. The calls in flight on this node are kept in a map per shard, so a duplicate on
// another connection of the node joins the call. The done records are kept per shard, oldest first: records
// expire in that order, since they share one TTL. Records are not shared between nodes.
//
// Budgets. A record counts recordOverhead plus its reply bytes. When a shard is over its entry or byte share, the
// new record first evicts the oldest record of its own scope, and only when its scope has none, the oldest record
// of the shard. A flood by one scope therefore evicts its own records. The per-scope cap is enforced the same way.
type Memory struct {
	seed          maphash.Seed
	shards        []shard
	maxReplyBytes int
	entries       atomic.Int64
}

// shard is one part of the store. Its fields are guarded by mu.
type shard struct {
	mu         sync.Mutex
	done       map[Key]*storedRecord
	flights    map[Key]*Flight
	order      *list.List
	scopes     map[string]*scopeState
	bytes      int64
	maxEntries int
	maxBytes   int64
	maxScope   int
	ttl        time.Duration
	entries    *atomic.Int64
}

// storedRecord is a done record in a shard, with its place in the shard order and in its scope order.
type storedRecord struct {
	key       Key
	rec       Record
	expireAt  time.Time
	size      int64
	elem      *list.Element
	scopeElem *list.Element
}

// scopeState holds the records of one scope, oldest first.
type scopeState struct {
	order *list.List
}

// NewMemory returns an empty node-local store.
func NewMemory(opts MemoryOptions) *Memory {
	m := &Memory{
		seed:          maphash.MakeSeed(),
		shards:        make([]shard, memoryShards),
		maxReplyBytes: opts.MaxReplyBytes,
	}
	for i := range m.shards {
		s := &m.shards[i]
		s.done = map[Key]*storedRecord{}
		s.flights = map[Key]*Flight{}
		s.order = list.New()
		s.scopes = map[string]*scopeState{}
		s.maxEntries = shareOf(opts.MaxEntries)
		s.maxBytes = int64(shareOf(int(opts.MaxBytes)))
		s.maxScope = opts.MaxRecordsPerScope
		s.ttl = opts.TTL
		s.entries = &m.entries
	}
	return m
}

// shareOf returns the share of a shard of a limit, rounded up. A zero limit stays zero, which is not enforced.
func shareOf(limit int) int {
	if limit <= 0 {
		return 0
	}
	return (limit + memoryShards - 1) / memoryShards
}

// shardOf returns the shard of scope.
func (m *Memory) shardOf(scope string) *shard {
	return &m.shards[maphash.String(m.seed, scope)%memoryShards]
}

// Begin implements Store.
func (m *Memory) Begin(key Key, flight *Flight) (Lookup, error) {
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep(time.Now())
	if r, found := s.done[key]; found {
		if r.rec.Fingerprint != zeroFingerprint && r.rec.Fingerprint != flight.fingerprint {
			return Lookup{Outcome: OutcomeMismatch}, nil
		}
		return Lookup{Outcome: OutcomeDone, Record: r.rec}, nil
	}
	if running, found := s.flights[key]; found {
		if running.fingerprint != flight.fingerprint {
			return Lookup{Outcome: OutcomeMismatch}, nil
		}
		return Lookup{Outcome: OutcomeInFlight, Flight: running}, nil
	}
	s.flights[key] = flight
	return Lookup{Outcome: OutcomeMiss}, nil
}

// Complete implements Store. The record is kept before the flight is removed from the map and woken up, so a
// call which misses the done records always finds the flight.
func (m *Memory) Complete(key Key, flight *Flight, rec Record) error {
	rec.Fingerprint = flight.fingerprint
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	if s.flights[key] == flight {
		delete(s.flights, key)
	}
	if !rec.Abandoned {
		kept := rec
		if rec.Err == nil && len(rec.Reply) > m.maxReplyBytes {
			kept = Record{Uncached: true, Fingerprint: rec.Fingerprint}
		}
		now := time.Now()
		s.sweep(now)
		s.insert(key, kept, now)
	}
	s.mu.Unlock()
	flight.finish(rec)
	return nil
}

// Cancel implements Store. A flight is cancelled outside the lock. Otherwise the key is kept as cancelled, which
// counts toward the budgets like any record.
func (m *Memory) Cancel(key Key, failure *centrifuge.Error) error {
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	if running, found := s.flights[key]; found {
		s.mu.Unlock()
		running.Cancel()
		return nil
	}
	now := time.Now()
	s.sweep(now)
	if _, found := s.done[key]; !found {
		s.insert(key, Record{Err: failure}, now)
	}
	s.mu.Unlock()
	return nil
}

// Ack implements Store.
func (m *Memory) Ack(scope string, msgIDs []uint64) error {
	s := m.shardOf(scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep(time.Now())
	for _, msgID := range msgIDs {
		if r, found := s.done[Key{Scope: scope, MsgID: msgID}]; found {
			s.remove(r)
		}
	}
	return nil
}

// Len implements Store. It is the number of records kept, which may include records which expired, until their
// shard is next accessed.
func (m *Memory) Len() int {
	return int(m.entries.Load())
}

// Bytes returns the bytes of the records kept. Expired records count until their shard is next accessed.
func (m *Memory) Bytes() int64 {
	var total int64
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		total += s.bytes
		s.mu.Unlock()
	}
	return total
}

// sweep removes the records of the shard which expired at now. Caller holds mu.
func (s *shard) sweep(now time.Time) {
	for e := s.order.Front(); e != nil; e = s.order.Front() {
		r := e.Value.(*storedRecord)
		if now.Before(r.expireAt) {
			return
		}
		s.remove(r)
	}
}

// insert keeps rec for key, making room within the budgets of the shard. Caller holds mu, and key has no record.
func (s *shard) insert(key Key, rec Record, now time.Time) {
	size := recordSize(rec)
	if s.maxBytes > 0 && size > s.maxBytes {
		// A record larger than a whole share is kept as uncached, so that a retransmit is still not run again.
		rec = Record{Uncached: true, Fingerprint: rec.Fingerprint}
		size = recordOverhead
	}
	if s.maxScope > 0 {
		for sc := s.scopes[key.Scope]; sc != nil && sc.order.Len() >= s.maxScope; sc = s.scopes[key.Scope] {
			s.remove(sc.order.Front().Value.(*storedRecord))
		}
	}
	for s.overBudget(size) {
		victim := s.victimFor(key.Scope)
		if victim == nil {
			break
		}
		s.remove(victim)
	}
	r := &storedRecord{key: key, rec: rec, expireAt: now.Add(s.ttl), size: size}
	r.elem = s.order.PushBack(r)
	sc := s.scopes[key.Scope]
	if sc == nil {
		sc = &scopeState{order: list.New()}
		s.scopes[key.Scope] = sc
	}
	r.scopeElem = sc.order.PushBack(r)
	s.done[key] = r
	s.bytes += size
	s.entries.Add(1)
}

// overBudget reports whether a record of size would go over the entry or byte share of the shard.
func (s *shard) overBudget(size int64) bool {
	if s.maxEntries > 0 && len(s.done) >= s.maxEntries {
		return true
	}
	return s.maxBytes > 0 && s.bytes+size > s.maxBytes
}

// victimFor returns the record to evict for a new record of scope: the oldest of its scope, or else the oldest
// of the shard. It returns nil when the shard has no record.
func (s *shard) victimFor(scope string) *storedRecord {
	if sc := s.scopes[scope]; sc != nil && sc.order.Len() > 0 {
		return sc.order.Front().Value.(*storedRecord)
	}
	if e := s.order.Front(); e != nil {
		return e.Value.(*storedRecord)
	}
	return nil
}

// remove drops a done record from the shard. Caller holds mu.
func (s *shard) remove(r *storedRecord) {
	delete(s.done, r.key)
	s.order.Remove(r.elem)
	sc := s.scopes[r.key.Scope]
	sc.order.Remove(r.scopeElem)
	if sc.order.Len() == 0 {
		delete(s.scopes, r.key.Scope)
	}
	s.bytes -= r.size
	s.entries.Add(-1)
}

// recordSize returns the size counted for a record.
func recordSize(rec Record) int64 {
	size := int64(recordOverhead + len(rec.Reply))
	if rec.Err != nil {
		size += int64(len(rec.Err.Message))
	}
	return size
}
