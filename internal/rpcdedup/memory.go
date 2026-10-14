package rpcdedup

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
)

// defaultShards is the number of shards of the node-local store when MemoryOptions.Shards is zero.
const defaultShards = 64

// recordOverhead is the size counted for a record besides its reply or error message, so that records without a
// reply count too.
const recordOverhead = 128

// zeroFingerprint is the fingerprint of a record which has no call, such as a cancelled one.
var zeroFingerprint [32]byte

// sweepLimit is the number of expired records which one access removes at most. The insert loop removes the
// expired records it needs before it evicts a live one, so the cap delays no budget.
const sweepLimit = 64

// Pool indexes. The authenticated pool holds the scopes of users, the anonymous pool the scopes of anonymous
// connections, so that a flood of anonymous instances cannot refuse or evict an authenticated scope.
const (
	poolAuthenticated = 0
	poolAnonymous     = 1
	poolCount         = 2
)

// Reasons of an eviction, the values of EvictionObserver.Evicted.
const (
	// EvictionScopeCap is the oldest record of a scope which is over MaxRecordsPerScope.
	EvictionScopeCap = "scope_cap"
	// EvictionOverReserve is the oldest record of the inserting scope, which is over its reserve while its pool is
	// full.
	EvictionOverReserve = "over_reserve"
	// EvictionSurplus is the oldest record of the scope which has been over its reserve the longest, evicted for
	// another scope.
	EvictionSurplus = "surplus"
	// EvictionUncached is a reply which is dropped to fit the budget: the record is kept without it.
	EvictionUncached = "uncached"
)

// EvictionObserver is told of each record which the store evicts to make room. It is called with the shard lock
// held, so it must not block.
type EvictionObserver interface {
	Evicted(reason string)
}

// MemoryOptions configures the node-local store. The budgets are shared out over the shards, and the anonymous
// part of each budget is kept for the scopes of anonymous connections.
type MemoryOptions struct {
	// MaxEntries bounds the records kept by the node, anonymous ones included.
	MaxEntries int
	// MaxBytes bounds the bytes of the records kept by the node, anonymous ones included.
	MaxBytes int64
	// AnonymousMaxEntries is the part of MaxEntries kept for the scopes of anonymous connections.
	AnonymousMaxEntries int
	// AnonymousMaxBytes is the part of MaxBytes kept for the scopes of anonymous connections.
	AnonymousMaxBytes int64
	// MaxRecordsPerScope bounds the records kept for one scope. The oldest record of the scope goes first.
	MaxRecordsPerScope int
	// ScopeReserve is the number of records every admitted scope keeps, which no other scope can evict.
	ScopeReserve int
	// ScopeReserveBytes is the number of bytes every admitted scope keeps, which no other scope can evict.
	ScopeReserveBytes int64
	// MaxScopesPerUser bounds the authenticated scopes of one user which hold records or calls on the node.
	MaxScopesPerUser int
	// TTL is how long a record is kept.
	TTL time.Duration
	// MaxReplyBytes is the size above which a reply is not kept.
	MaxReplyBytes int
	// Shards is the number of shards. Zero means 64. Tests use one shard, so that the budgets are whole.
	Shards int
	// Now returns the current time. Nil means time.Now.
	Now func() time.Time
	// Observer is told of each eviction. Nil means none.
	Observer EvictionObserver
}

// Memory is the node-local store. The calls in flight on this node are kept in a map per shard, so a duplicate on
// another connection of the node joins the call. The records are kept per shard, in the order of their creation,
// since they share one TTL. Records are not shared between nodes.
//
// Scopes and budgets. A scope is admitted when it first holds a record or a call, and is released when it holds
// neither. An admitted scope is guaranteed ScopeReserve records and ScopeReserveBytes bytes, which no other scope
// can evict. A pool admits at most maxEntries/ScopeReserve scopes, so the reserves always fit its budget. Above
// its reserve a scope keeps records as long as the budget has room, and when the budget is full the store makes
// room in this order: the oldest record of the inserting scope while it is over its reserve, then the oldest
// record of the scope which has been over its reserve the longest, and otherwise the reply is dropped and the
// record kept as uncached.
//
// Shards. A scope with a user is kept in the shard of its user, so the scope limit of a user is one counter under
// one lock.
type Memory struct {
	seed          maphash.Seed
	shards        []shard
	maxReplyBytes int
	stats         [poolCount]poolStats
}

// poolStats are the gauges of a pool, read without the shard locks.
type poolStats struct {
	entries atomic.Int64
	bytes   atomic.Int64
	scopes  atomic.Int64
}

// shard is one part of the store. Its fields are guarded by mu.
type shard struct {
	mu               sync.Mutex
	done             map[Key]*storedRecord
	flights          map[Key]*Flight
	scopes           map[string]*scopeState
	users            map[string]int
	pools            [poolCount]pool
	order            recordList
	ttl              time.Duration
	maxScope         int
	maxScopesPerUser int
	now              func() time.Time
	observer         EvictionObserver
}

// pool is the budget of one kind of scope in a shard. Its surplus scopes are the scopes over their reserve, oldest
// first.
type pool struct {
	maxEntries     int
	maxBytes       int64
	maxScopes      int
	reserveEntries int
	reserveBytes   int64
	entries        int
	bytes          int64
	scopes         int
	surplus        scopeList
	stats          *poolStats
}

// recordLinks are the neighbours of a record in one of its lists.
type recordLinks struct {
	prev *storedRecord
	next *storedRecord
}

// recordList is a list of records, linked through the links which links returns for each record.
type recordList struct {
	head  *storedRecord
	tail  *storedRecord
	len   int
	links func(*storedRecord) *recordLinks
}

// storedRecord is a done record in a shard, with its place in the shard order and in the order of its scope.
type storedRecord struct {
	key      Key
	rec      Record
	expireAt time.Time
	size     int64
	scope    *scopeState
	byAge    recordLinks
	byScope  recordLinks
}

// scopeState is an admitted scope: its records, oldest first, and its running calls.
type scopeState struct {
	scope       string
	user        string
	pool        *pool
	records     recordList
	bytes       int64
	flights     int
	surplus     bool
	surplusPrev *scopeState
	surplusNext *scopeState
}

// scopeList is the list of the surplus scopes of a pool, linked through their surplus neighbours.
type scopeList struct {
	head *scopeState
	tail *scopeState
}

// NewMemory returns an empty node-local store.
func NewMemory(opts MemoryOptions) *Memory {
	shards := opts.Shards
	if shards <= 0 {
		shards = defaultShards
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	m := &Memory{
		seed:          maphash.MakeSeed(),
		shards:        make([]shard, shards),
		maxReplyBytes: opts.MaxReplyBytes,
	}
	authEntries := shareOf(int64(opts.MaxEntries)-int64(opts.AnonymousMaxEntries), shards)
	authBytes := shareOf(opts.MaxBytes-opts.AnonymousMaxBytes, shards)
	anonEntries := shareOf(int64(opts.AnonymousMaxEntries), shards)
	anonBytes := shareOf(opts.AnonymousMaxBytes, shards)
	for i := range m.shards {
		s := &m.shards[i]
		s.done = map[Key]*storedRecord{}
		s.flights = map[Key]*Flight{}
		s.scopes = map[string]*scopeState{}
		s.users = map[string]int{}
		s.order = recordList{links: byAgeLinks}
		s.ttl = opts.TTL
		s.maxScope = opts.MaxRecordsPerScope
		s.maxScopesPerUser = opts.MaxScopesPerUser
		s.now = now
		s.observer = opts.Observer
		s.pools[poolAuthenticated] = newPool(authEntries, authBytes, opts, &m.stats[poolAuthenticated])
		s.pools[poolAnonymous] = newPool(anonEntries, anonBytes, opts, &m.stats[poolAnonymous])
	}
	return m
}

// shareOf returns the share of one shard of a limit. A limit which is not positive has no share.
func shareOf(limit int64, shards int) int64 {
	if limit <= 0 {
		return 0
	}
	return limit / int64(shards)
}

// newPool returns the budget of a pool of one shard, with the scope limit which the reserves allow. The reserves
// are at least one record and one byte, so the division is defined.
func newPool(entries int64, bytes int64, opts MemoryOptions, stats *poolStats) pool {
	reserveEntries := max(opts.ScopeReserve, 1)
	reserveBytes := max(opts.ScopeReserveBytes, 1)
	maxScopes := entries / int64(reserveEntries)
	if byBytes := bytes / reserveBytes; byBytes < maxScopes {
		maxScopes = byBytes
	}
	return pool{
		maxEntries:     int(entries),
		maxBytes:       bytes,
		maxScopes:      int(maxScopes),
		reserveEntries: reserveEntries,
		reserveBytes:   reserveBytes,
		stats:          stats,
	}
}

// byAgeLinks returns the links of a record in the order of the shard.
func byAgeLinks(r *storedRecord) *recordLinks {
	return &r.byAge
}

// byScopeLinks returns the links of a record in the order of its scope.
func byScopeLinks(r *storedRecord) *recordLinks {
	return &r.byScope
}

// pushBack appends r to the list.
func (l *recordList) pushBack(r *storedRecord) {
	links := l.links(r)
	links.prev = l.tail
	links.next = nil
	if l.tail != nil {
		l.links(l.tail).next = r
	} else {
		l.head = r
	}
	l.tail = r
	l.len++
}

// remove unlinks r, which is in the list.
func (l *recordList) remove(r *storedRecord) {
	links := l.links(r)
	if links.prev != nil {
		l.links(links.prev).next = links.next
	} else {
		l.head = links.next
	}
	if links.next != nil {
		l.links(links.next).prev = links.prev
	} else {
		l.tail = links.prev
	}
	links.prev = nil
	links.next = nil
	l.len--
}

// front returns the oldest record of the list, or nil when it is empty.
func (l *recordList) front() *storedRecord {
	return l.head
}

// pushBack appends sc to the surplus scopes.
func (l *scopeList) pushBack(sc *scopeState) {
	sc.surplusPrev = l.tail
	sc.surplusNext = nil
	if l.tail != nil {
		l.tail.surplusNext = sc
	} else {
		l.head = sc
	}
	l.tail = sc
}

// remove unlinks sc, which is in the surplus scopes.
func (l *scopeList) remove(sc *scopeState) {
	if sc.surplusPrev != nil {
		sc.surplusPrev.surplusNext = sc.surplusNext
	} else {
		l.head = sc.surplusNext
	}
	if sc.surplusNext != nil {
		sc.surplusNext.surplusPrev = sc.surplusPrev
	} else {
		l.tail = sc.surplusPrev
	}
	sc.surplusPrev = nil
	sc.surplusNext = nil
}

// front returns the surplus scope which has been over its reserve the longest, or nil when there is none.
func (l *scopeList) front() *scopeState {
	return l.head
}

// shardOf returns the shard of scope: the shard of its user for an authenticated scope, so that all scopes of a
// user are in one shard, and the shard of the scope itself for an anonymous one.
func (m *Memory) shardOf(scope string) *shard {
	key := scope
	if user := scopeUser(scope); user != "" {
		key = user
	}
	return &m.shards[maphash.String(m.seed, key)%uint64(len(m.shards))]
}

// Begin implements Store.
func (m *Memory) Begin(key Key, flight *Flight) (Lookup, error) {
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepSome(now)
	if r := s.liveRecord(key, now); r != nil {
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
	sc, err := s.admit(key.Scope)
	if err != nil {
		return Lookup{}, err
	}
	sc.flights++
	s.flights[key] = flight
	return Lookup{Outcome: OutcomeMiss}, nil
}

// Complete implements Store. The record is kept before the flight is removed from the map and woken up, so a
// call which misses the done records always finds the flight. Only the owner of the flight keeps a record.
func (m *Memory) Complete(key Key, flight *Flight, rec Record) error {
	rec.Fingerprint = flight.fingerprint
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	sc := s.scopes[key.Scope]
	if sc != nil && s.flights[key] == flight {
		delete(s.flights, key)
		sc.flights--
		if !rec.Abandoned {
			kept := rec
			if rec.Err == nil && len(rec.Reply) > m.maxReplyBytes {
				kept = Record{Uncached: true, Fingerprint: rec.Fingerprint}
			}
			s.keep(sc, key, kept, s.now())
		}
	}
	if sc != nil {
		s.releaseIfEmpty(sc)
	}
	s.mu.Unlock()
	flight.finish(rec)
	return nil
}

// Cancel implements Store. A flight is cancelled outside the lock. Otherwise the key is kept as cancelled, which
// counts toward the budgets like any record, and its scope is admitted like the scope of any call.
func (m *Memory) Cancel(key Key, failure *centrifuge.Error) error {
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	if running, found := s.flights[key]; found {
		s.mu.Unlock()
		running.Cancel()
		return nil
	}
	now := s.now()
	s.sweepSome(now)
	if s.liveRecord(key, now) != nil {
		s.mu.Unlock()
		return nil
	}
	sc, err := s.admit(key.Scope)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.insert(sc, key, Record{Err: failure}, now)
	s.mu.Unlock()
	return nil
}

// Ack implements Store.
func (m *Memory) Ack(scope string, msgIDs []uint64) error {
	s := m.shardOf(scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepSome(s.now())
	for _, msgID := range msgIDs {
		if r := s.done[Key{Scope: scope, MsgID: msgID}]; r != nil {
			s.remove(r)
		}
	}
	return nil
}

// Len implements Store. It is the number of records kept, which may include records which expired, until their
// shard is next accessed.
func (m *Memory) Len() int {
	return int(m.Stats().Entries)
}

// Stats implements Store. It reads the gauges of the pools without taking the shard locks.
func (m *Memory) Stats() Stats {
	var out Stats
	out.Authenticated = m.stats[poolAuthenticated].read()
	out.Anonymous = m.stats[poolAnonymous].read()
	out.Entries = out.Authenticated.Entries + out.Anonymous.Entries
	out.Bytes = out.Authenticated.Bytes + out.Anonymous.Bytes
	return out
}

// read returns the gauges of the pool.
func (p *poolStats) read() PoolStats {
	return PoolStats{
		Entries: p.entries.Load(),
		Bytes:   p.bytes.Load(),
		Scopes:  p.scopes.Load(),
	}
}

// poolFor returns the pool of scope in the shard.
func (s *shard) poolFor(scope string) *pool {
	if scopeAnonymous(scope) {
		return &s.pools[poolAnonymous]
	}
	return &s.pools[poolAuthenticated]
}

// admit returns the admitted scope of scope, admitting it when it is new. It returns a *RefusedError when the pool
// is at its scope limit, or the user is at MaxScopesPerUser. Caller holds mu.
func (s *shard) admit(scope string) (*scopeState, error) {
	if sc := s.scopes[scope]; sc != nil {
		return sc, nil
	}
	p := s.poolFor(scope)
	if p.scopes >= p.maxScopes {
		return nil, &RefusedError{Refusal: RefusalPool}
	}
	user := scopeUser(scope)
	if user != "" && s.users[user] >= s.maxScopesPerUser {
		return nil, &RefusedError{Refusal: RefusalUser}
	}
	sc := &scopeState{
		scope:   scope,
		user:    user,
		pool:    p,
		records: recordList{links: byScopeLinks},
	}
	s.scopes[scope] = sc
	p.scopes++
	p.stats.scopes.Add(1)
	if user != "" {
		s.users[user]++
	}
	return sc, nil
}

// releaseIfEmpty releases sc when it holds no record and no call: its pool slot and its user slot are freed.
// Caller holds mu.
func (s *shard) releaseIfEmpty(sc *scopeState) {
	if sc.records.len > 0 || sc.flights > 0 || s.scopes[sc.scope] != sc {
		return
	}
	delete(s.scopes, sc.scope)
	sc.pool.scopes--
	sc.pool.stats.scopes.Add(-1)
	if sc.user != "" {
		s.users[sc.user]--
		if s.users[sc.user] == 0 {
			delete(s.users, sc.user)
		}
	}
}

// liveRecord returns the done record of key when it has not expired. An expired record is removed, and nil is
// returned. Caller holds mu.
func (s *shard) liveRecord(key Key, now time.Time) *storedRecord {
	r := s.done[key]
	if r == nil {
		return nil
	}
	if now.Before(r.expireAt) {
		return r
	}
	s.remove(r)
	return nil
}

// sweepSome removes up to sweepLimit records which expired at now, from the oldest. Caller holds mu.
func (s *shard) sweepSome(now time.Time) {
	for i := 0; i < sweepLimit; i++ {
		r := s.order.front()
		if r == nil || now.Before(r.expireAt) {
			return
		}
		s.remove(r)
	}
}

// keep replaces the record of key with rec, the answer of the flight which sc owns. The scope is held while the
// old record is removed, so that it is not released before the new one is in. Caller holds mu.
func (s *shard) keep(sc *scopeState, key Key, rec Record, now time.Time) {
	sc.flights++
	if r := s.done[key]; r != nil {
		s.remove(r)
	}
	s.insert(sc, key, rec, now)
	sc.flights--
}

// insert keeps rec for key in scope sc, which is admitted, making room within the budgets of its pool. It never
// fails: when no room can be made, the record is kept as uncached. Caller holds mu, and key has no record.
func (s *shard) insert(sc *scopeState, key Key, rec Record, now time.Time) {
	p := sc.pool
	size := recordSize(rec)
	// The scope is held while its records are evicted: its last record may go before the new one is in.
	sc.flights++
	for sc.records.len > 0 && sc.records.len >= s.maxScope {
		s.evict(sc.records.front(), EvictionScopeCap)
	}
	for p.overBudget(size) {
		if f := s.order.front(); f != nil && !now.Before(f.expireAt) {
			s.remove(f)
			continue
		}
		if sc.records.len > 0 && (sc.records.len+1 > p.reserveEntries || sc.bytes+size > p.reserveBytes) {
			s.evict(sc.records.front(), EvictionOverReserve)
			continue
		}
		if v := p.surplus.front(); v != nil {
			s.evict(v.records.front(), EvictionSurplus)
			continue
		}
		if !rec.Uncached {
			rec = Record{Uncached: true, Fingerprint: rec.Fingerprint}
			size = recordOverhead
			s.observe(EvictionUncached)
			continue
		}
		// Unreachable: when this branch runs, the scope is within its reserve and no other scope is over its reserve,
		// so the pool holds at most scopes*reserve <= maxScopes*reserve <= maxEntries records, and at most
		// scopes*reserveBytes <= maxBytes bytes, leaving room for the record kept as uncached. The record is kept.
		break
	}
	r := &storedRecord{key: key, rec: rec, expireAt: now.Add(s.ttl), size: size, scope: sc}
	s.order.pushBack(r)
	sc.records.pushBack(r)
	sc.bytes += size
	s.done[key] = r
	p.entries++
	p.bytes += size
	p.stats.entries.Add(1)
	p.stats.bytes.Add(size)
	s.updateSurplus(sc)
	sc.flights--
}

// overBudget reports whether a record of size would go over the entry or byte budget of the pool.
func (p *pool) overBudget(size int64) bool {
	return p.entries >= p.maxEntries || p.bytes+size > p.maxBytes
}

// updateSurplus keeps sc in the surplus scopes exactly while it is over its reserve. A scope enters the list when it
// goes over its reserve, so the list is in the order in which the scopes went over. Caller holds mu.
func (s *shard) updateSurplus(sc *scopeState) {
	p := sc.pool
	want := sc.records.len > p.reserveEntries || sc.bytes > p.reserveBytes
	if want == sc.surplus {
		return
	}
	sc.surplus = want
	if want {
		p.surplus.pushBack(sc)
	} else {
		p.surplus.remove(sc)
	}
}

// evict removes r to make room for another record, and counts the eviction for reason. Caller holds mu.
func (s *shard) evict(r *storedRecord, reason string) {
	s.remove(r)
	s.observe(reason)
}

// observe counts an eviction for reason, when there is an observer. Caller holds mu.
func (s *shard) observe(reason string) {
	if s.observer != nil {
		s.observer.Evicted(reason)
	}
}

// remove drops a done record from the shard, and releases its scope when that holds nothing else. Caller holds mu.
func (s *shard) remove(r *storedRecord) {
	delete(s.done, r.key)
	s.order.remove(r)
	sc := r.scope
	sc.records.remove(r)
	sc.bytes -= r.size
	p := sc.pool
	p.entries--
	p.bytes -= r.size
	p.stats.entries.Add(-1)
	p.stats.bytes.Add(-r.size)
	s.updateSurplus(sc)
	s.releaseIfEmpty(sc)
}

// recordSize returns the size counted for a record: recordOverhead, plus the length of its reply, or of its error
// message when it has an error.
func recordSize(rec Record) int64 {
	size := int64(recordOverhead + len(rec.Reply))
	if rec.Err != nil {
		size += int64(len(rec.Err.Message))
	}
	return size
}
