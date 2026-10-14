package rpcdedup

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
)

// DefaultShards is the number of shards of the node-local store when MemoryOptions.Shards is zero.
const DefaultShards = 64

// RecordOverhead is the size counted for a record besides its reply or error message, so that records without a
// reply count too. It covers the storedRecord struct, which is 168 bytes on a 64-bit platform, and the entry of the
// done map of the shard which holds the record.
const RecordOverhead = 256

// zeroFingerprint is the fingerprint of a record which has no call, such as a cancelled one.
var zeroFingerprint [32]byte

// sweepLimit is the number of expired records which one access removes at most. The insert loop removes the
// expired records it needs before it evicts a live one, so the cap delays no budget.
const sweepLimit = 64

// admitSweeps is the number of sweeps which an admission makes, while the pool or the user of the scope is at its
// limit. The expired records which they remove release their scopes, so a new scope is refused only when the limit
// holds after them.
const admitSweeps = 4

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
	// EvictionUncached is a reply which is dropped to fit the share or the budget: the record is kept without it.
	EvictionUncached = "uncached"
	// EvictionReplySize is a reply over MaxReplyBytes, which is dropped when the call completes: the record is kept
	// without it.
	EvictionReplySize = "reply_size"
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
	// ScopeReserveBytes is the number of bytes every admitted scope keeps. Each record is kept up to
	// ScopeReserveBytes/ScopeReserve bytes, so the records of a scope within its reserve fit it.
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
// neither. An admitted scope is guaranteed ScopeReserve records, which no other scope can evict, and each of its
// records is kept only up to ScopeReserveBytes/ScopeReserve bytes, so the records of a scope within its reserve
// fit ScopeReserveBytes. A pool admits at most maxEntries/ScopeReserve scopes and at most maxBytes/ScopeReserveBytes
// scopes, so the reserves always fit its budgets. Above its reserve a scope keeps records as long as the budget has
// room, and when the budget is full the store makes room in this order: the oldest record of the inserting scope
// while it is over its reserve, then the oldest record of the scope which has been over its reserve the longest, and
// otherwise the reply is dropped and the record kept as uncached. Only a scope over its record reserve is evicted
// from, so a scope within its reserve keeps all its records.
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
	order            intrusiveList[storedRecord]
	ttl              time.Duration
	maxScope         int
	maxScopesPerUser int
	now              func() time.Time
	observer         EvictionObserver
}

// pool is the budget of one kind of scope in a shard. Its surplus scopes are the scopes over their record reserve,
// oldest first.
type pool struct {
	maxEntries     int
	maxBytes       int64
	maxScopes      int
	reserveEntries int
	recordBytes    int64
	entries        int
	bytes          int64
	scopes         int
	surplus        intrusiveList[scopeState]
	stats          *poolStats
}

// storedRecord is a done record in a shard, with its place in the shard order and in the order of its scope.
type storedRecord struct {
	key      Key
	rec      Record
	expireAt time.Time
	size     int64
	scope    *scopeState
	byAge    linkPair[storedRecord]
	byScope  linkPair[storedRecord]
}

// scopeState is an admitted scope: its records, oldest first, and its running calls.
type scopeState struct {
	scope        string
	user         string
	pool         *pool
	records      intrusiveList[storedRecord]
	bytes        int64
	flights      int
	surplus      bool
	surplusLinks linkPair[scopeState]
}

// NewMemory returns an empty node-local store.
func NewMemory(opts MemoryOptions) *Memory {
	shards := opts.Shards
	if shards <= 0 {
		shards = DefaultShards
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
		s.order = intrusiveList[storedRecord]{links: byAgeLinks}
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
// are at least one record and one byte, so the division is defined. A record is kept up to its share of the byte
// reserve, reserveBytes/reserveEntries, so the records of a scope within its reserve fit the byte reserve. Config
// validation keeps that share at least RecordOverhead, so a record kept as uncached always fits it.
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
		recordBytes:    reserveBytes / int64(reserveEntries),
		surplus:        intrusiveList[scopeState]{links: surplusLinks},
		stats:          stats,
	}
}

// byAgeLinks returns the links of a record in the order of the shard.
func byAgeLinks(r *storedRecord) *linkPair[storedRecord] {
	return &r.byAge
}

// byScopeLinks returns the links of a record in the order of its scope.
func byScopeLinks(r *storedRecord) *linkPair[storedRecord] {
	return &r.byScope
}

// surplusLinks returns the links of a scope in the surplus scopes of its pool.
func surplusLinks(sc *scopeState) *linkPair[scopeState] {
	return &sc.surplusLinks
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
	sc, err := s.admit(key.Scope, now)
	if err != nil {
		return Lookup{}, err
	}
	sc.flights++
	s.flights[key] = flight
	return Lookup{Outcome: OutcomeMiss}, nil
}

// Complete implements Store. The flight is removed and the record kept in one critical section, so a call which
// misses the done records always finds the flight or the record. The duplicates which joined the flight are answered
// with the record as it is kept, so a reply which is kept as uncached is evicted for them too. Only the owner of the
// flight keeps a record.
func (m *Memory) Complete(key Key, flight *Flight, rec Record) error {
	rec.Fingerprint = flight.fingerprint
	oversize := rec.Err == nil && len(rec.Reply) > m.maxReplyBytes
	kept := rec
	if oversize {
		kept = Record{Uncached: true, Fingerprint: rec.Fingerprint}
	}
	s := m.shardOf(key.Scope)
	s.mu.Lock()
	sc := s.scopes[key.Scope]
	if sc != nil && s.flights[key] == flight {
		delete(s.flights, key)
		sc.flights--
		if !rec.Abandoned {
			if oversize {
				s.observe(EvictionReplySize)
			}
			kept = s.insert(sc, key, kept, s.now())
		}
	}
	if sc != nil {
		s.releaseIfEmpty(sc)
	}
	s.mu.Unlock()
	flight.finish(kept)
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
	sc, err := s.admit(key.Scope, now)
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

// admit returns the admitted scope of scope, admitting it when it is new. While the pool of the scope is at its scope
// limit, or its user is at MaxScopesPerUser, it sweeps the expired records, up to admitSweeps times, since the scopes
// they hold are released. It returns a *RefusedError when a limit still holds. Caller holds mu.
func (s *shard) admit(scope string, now time.Time) (*scopeState, error) {
	if sc := s.scopes[scope]; sc != nil {
		return sc, nil
	}
	p := s.poolFor(scope)
	user := scopeUser(scope)
	refusal, refused := s.refusalOf(p, user)
	for i := 0; refused && i < admitSweeps; i++ {
		s.sweepSome(now)
		refusal, refused = s.refusalOf(p, user)
	}
	if refused {
		return nil, &RefusedError{Refusal: refusal}
	}
	sc := &scopeState{
		scope:   scope,
		user:    user,
		pool:    p,
		records: intrusiveList[storedRecord]{links: byScopeLinks},
	}
	s.scopes[scope] = sc
	p.scopes++
	p.stats.scopes.Add(1)
	if user != "" {
		s.users[user]++
	}
	return sc, nil
}

// refusalOf returns the refusal which a new scope of pool p and user would meet, and whether it meets one. Caller
// holds mu.
func (s *shard) refusalOf(p *pool, user string) (Refusal, bool) {
	if p.scopes >= p.maxScopes {
		return RefusalPool, true
	}
	if user != "" && s.users[user] >= s.maxScopesPerUser {
		return RefusalUser, true
	}
	return "", false
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

// insert keeps rec for key in scope sc, which is admitted, making room within the budgets of its pool, and returns
// the record as it is kept. A reply over the share of a record is kept as uncached, and so is one which fits no room:
// it never fails. A record which key still holds is removed first, so the key holds one record. Caller holds mu.
// No record of key exists under its flight: Begin removes an expired record and answers a live one without a flight,
// and only the owner of the flight inserts. The removal above keeps the counts right if that ever changes.
func (s *shard) insert(sc *scopeState, key Key, rec Record, now time.Time) Record {
	p := sc.pool
	size := recordSize(rec)
	if !rec.Uncached && size > p.recordBytes {
		rec = Record{Uncached: true, Fingerprint: rec.Fingerprint}
		size = RecordOverhead
		s.observe(EvictionUncached)
	}
	// The scope is held while its records are evicted: its last record may go before the new one is in.
	sc.flights++
	if old := s.done[key]; old != nil {
		s.drop(old, sc)
	}
	for sc.records.len > 0 && sc.records.len >= s.maxScope {
		s.evict(sc.records.front(), EvictionScopeCap, sc)
	}
	for p.overBudget(size) {
		if f := s.order.front(); f != nil && !now.Before(f.expireAt) {
			s.drop(f, sc)
			continue
		}
		if sc.records.len > 0 && sc.records.len+1 > p.reserveEntries {
			s.evict(sc.records.front(), EvictionOverReserve, sc)
			continue
		}
		if v := p.surplus.front(); v != nil {
			s.evict(v.records.front(), EvictionSurplus, sc)
			continue
		}
		if !rec.Uncached {
			rec = Record{Uncached: true, Fingerprint: rec.Fingerprint}
			size = RecordOverhead
			s.observe(EvictionUncached)
			continue
		}
		// Unreachable: when this branch runs, no record is expired, and the scope is within its record reserve,
		// so it holds at most reserveEntries records of at most recordBytes each, and fits reserveBytes. No other
		// scope of the pool is over its record reserve, so each of them fits its reserves too. The pool holds at most
		// scopes*reserveEntries <= maxScopes*reserveEntries <= maxEntries records, and at most
		// scopes*reserveBytes <= maxScopes*reserveBytes <= maxBytes bytes, the new record included. That contradicts
		// overBudget, so the record is kept.
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
	return rec
}

// overBudget reports whether a record of size would go over the entry or byte budget of the pool.
func (p *pool) overBudget(size int64) bool {
	return p.entries >= p.maxEntries || p.bytes+size > p.maxBytes
}

// updateSurplus keeps sc in the surplus scopes exactly while it holds more records than its reserve. A scope enters
// the list when it goes over its reserve, so the list is in the order in which the scopes went over. The bytes of a
// scope do not make it surplus: a scope within its record reserve keeps all its records, whatever their bytes. Caller
// holds mu.
func (s *shard) updateSurplus(sc *scopeState) {
	p := sc.pool
	want := sc.records.len > p.reserveEntries
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

// evict removes r to make room for a record of inserting, and counts the eviction for reason. Caller holds mu.
func (s *shard) evict(r *storedRecord, reason string, inserting *scopeState) {
	s.drop(r, inserting)
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
	s.unlink(r)
	s.updateSurplus(r.scope)
	s.releaseIfEmpty(r.scope)
}

// drop is remove for a record which makes room for a record of inserting. The surplus membership of inserting is
// left to insert, which sets it once the new record is in: a scope which drops one record and takes another stays
// where it is in the surplus scopes, so its place in the order of going over its reserve is kept. The scope of a
// record being dropped holds a flight while it is inserted, so it is not released. Caller holds mu.
func (s *shard) drop(r *storedRecord, inserting *scopeState) {
	s.unlink(r)
	if r.scope != inserting {
		s.updateSurplus(r.scope)
		s.releaseIfEmpty(r.scope)
	}
}

// unlink removes a done record from the shard, from the order of the shard and of its scope, and from the counts of
// its scope and pool. Caller holds mu.
func (s *shard) unlink(r *storedRecord) {
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
}

// recordSize returns the size counted for a record: RecordOverhead, plus the length of its reply, or of its error
// message when it has an error.
func recordSize(rec Record) int64 {
	size := int64(RecordOverhead + len(rec.Reply))
	if rec.Err != nil {
		size += int64(len(rec.Err.Message))
	}
	return size
}
