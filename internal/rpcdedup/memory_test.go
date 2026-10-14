package rpcdedup

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

var testCancelled *centrifuge.Error = &centrifuge.Error{Code: 107, Message: "cancelled"}

// testClock is the clock of the store tests. It only moves when a test advances it.
type testClock struct {
	t time.Time
}

// now returns the current time of the clock.
func (c *testClock) now() time.Time {
	return c.t
}

// advance moves the clock forward by d.
func (c *testClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

// countingObserver counts the evictions by reason.
type countingObserver struct {
	counts map[string]int
}

// Evicted implements EvictionObserver.
func (o *countingObserver) Evicted(reason string) {
	o.counts[reason]++
}

// newCountingObserver returns an observer with no evictions.
func newCountingObserver() *countingObserver {
	return &countingObserver{counts: map[string]int{}}
}

// testMemoryOptions returns options with budgets large enough for the store tests, one shard, and the clock.
// The reserve is the default: 4 records and 8 KiB per scope.
func testMemoryOptions(clock *testClock, maxReply int) MemoryOptions {
	return MemoryOptions{
		MaxEntries:         1000,
		MaxBytes:           1 << 30,
		MaxRecordsPerScope: 1024,
		ScopeReserve:       4,
		ScopeReserveBytes:  8192,
		MaxScopesPerUser:   8,
		TTL:                time.Minute,
		MaxReplyBytes:      maxReply,
		Shards:             1,
		Now:                clock.now,
	}
}

// newTestMemory returns a store of testMemoryOptions and its clock.
func newTestMemory(maxReply int) (*Memory, *testClock) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	return NewMemory(testMemoryOptions(clock, maxReply)), clock
}

// testScope returns the scope of the session id of user, as an authenticated connection has it.
func testScope(user string, id string) string {
	return ScopeOf("s", user, []byte(id))
}

// testBegin runs Begin for key and fails the test on an error.
func testBegin(t *testing.T, store Store, key Key, flight *Flight) Lookup {
	t.Helper()
	lookup, err := store.Begin(key, flight)
	require.NoError(t, err)
	return lookup
}

// testComplete keeps the answer of flight for key with the reply data.
func testComplete(t *testing.T, store Store, key Key, flight *Flight, data string) {
	t.Helper()
	require.NoError(t, store.Complete(key, flight, Record{Reply: []byte(data)}))
}

// testAnswer runs a call of key and keeps its reply, as the client does for a miss.
func testAnswer(t *testing.T, store Store, key Key, data string) {
	t.Helper()
	flight := NewFlight()
	require.Equal(t, OutcomeMiss, testBegin(t, store, key, flight).Outcome)
	testComplete(t, store, key, flight, data)
}

// testIsMiss reports whether key is a miss. A miss is left as an abandoned call, so nothing is kept for it.
func testIsMiss(t *testing.T, store Store, key Key) bool {
	t.Helper()
	flight := NewFlight()
	lookup := testBegin(t, store, key, flight)
	if lookup.Outcome != OutcomeMiss {
		return false
	}
	require.NoError(t, store.Complete(key, flight, Record{Abandoned: true}))
	return true
}

// testDone reports whether key is done, and returns its record when it is.
func testDone(t *testing.T, store Store, key Key) (Record, bool) {
	t.Helper()
	lookup := testBegin(t, store, key, NewFlight())
	return lookup.Record, lookup.Outcome == OutcomeDone
}

// testEntries returns the number of records the store keeps, read from its gauges.
func testEntries(store Store) int {
	return int(store.Stats().Entries)
}

// testRecordCount returns the number of records kept for scope.
func testRecordCount(store *Memory, scope string) int {
	s := store.shardOf(scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	sc := s.scopes[scope]
	if sc == nil {
		return 0
	}
	return sc.records.len
}

// testRefused returns the refusal of err, and fails the test when err is not a refusal.
func testRefused(t *testing.T, err error) Refusal {
	t.Helper()
	var refused *RefusedError
	require.ErrorAs(t, err, &refused)
	return refused.Refusal
}

// checkInvariants walks the store under its locks, and fails the test when a count, a list or a budget is off.
func checkInvariants(t *testing.T, store *Memory) {
	t.Helper()
	var totals [poolCount]poolTotals
	for i := range store.shards {
		s := &store.shards[i]
		s.mu.Lock()
		shardTotals := checkShard(t, s)
		s.mu.Unlock()
		for pi := range totals {
			totals[pi].entries += shardTotals[pi].entries
			totals[pi].bytes += shardTotals[pi].bytes
			totals[pi].scopes += shardTotals[pi].scopes
		}
	}
	var entries, bytes int64
	for pi := range totals {
		gauges := store.stats[pi].read()
		require.Equal(t, totals[pi].entries, gauges.Entries)
		require.Equal(t, totals[pi].bytes, gauges.Bytes)
		require.Equal(t, totals[pi].scopes, gauges.Scopes)
		entries += totals[pi].entries
		bytes += totals[pi].bytes
	}
	stats := store.Stats()
	require.Equal(t, entries, stats.Entries)
	require.Equal(t, bytes, stats.Bytes)
	require.Equal(t, int(entries), testEntries(store))
}

// poolTotals are the counts of one pool, summed by checkInvariants.
type poolTotals struct {
	entries int64
	bytes   int64
	scopes  int64
}

// checkShard checks the structures of one shard, which the caller holds, and returns the counts of its pools.
func checkShard(t *testing.T, s *shard) [poolCount]poolTotals {
	t.Helper()
	var totals [poolCount]poolTotals
	var surplus [poolCount]int
	users := map[string]int{}
	flightsOf := map[string]int{}
	for key := range s.flights {
		flightsOf[key.Scope]++
		require.Nil(t, s.done[key], "a call in flight has no record")
	}

	orderLen := 0
	for r := s.order.head; r != nil; r = r.byAge.next {
		orderLen++
		require.Same(t, r, s.done[r.key])
	}
	require.Equal(t, orderLen, s.order.len)
	require.Equal(t, len(s.done), orderLen)

	for name, sc := range s.scopes {
		require.Equal(t, name, sc.scope)
		pi := poolAuthenticated
		if scopeAnonymous(name) {
			pi = poolAnonymous
		}
		p := &s.pools[pi]
		require.Same(t, p, sc.pool)
		require.Equal(t, scopeUser(name), sc.user)
		if sc.user != "" {
			users[sc.user]++
		}
		require.Equal(t, flightsOf[name], sc.flights)
		require.True(t, sc.records.len > 0 || sc.flights > 0, "an admitted scope holds a record or a call")

		n := 0
		var bytes int64
		for r := sc.records.head; r != nil; r = r.byScope.next {
			n++
			bytes += r.size
			require.LessOrEqual(t, r.size, p.recordBytes, "a record is kept within its share of the reserve")
			require.Same(t, sc, r.scope)
			require.Same(t, r, s.done[r.key])
		}
		require.Equal(t, sc.records.len, n)
		require.Equal(t, sc.bytes, bytes)
		if n <= p.reserveEntries {
			require.LessOrEqual(t, bytes, int64(p.reserveEntries)*p.recordBytes, "a scope within its record reserve fits its byte reserve")
		}

		over := n > p.reserveEntries
		require.Equal(t, over, sc.surplus)
		if over {
			surplus[pi]++
		}
		totals[pi].entries += int64(n)
		totals[pi].bytes += bytes
		totals[pi].scopes++
	}

	for pi := range s.pools {
		p := &s.pools[pi]
		require.Equal(t, int64(p.entries), totals[pi].entries)
		require.Equal(t, p.bytes, totals[pi].bytes)
		require.Equal(t, p.scopes, int(totals[pi].scopes))
		require.LessOrEqual(t, p.scopes, p.maxScopes)
		require.LessOrEqual(t, p.entries, p.maxEntries)
		require.LessOrEqual(t, p.bytes, p.maxBytes)

		listed := 0
		for sc := p.surplus.head; sc != nil; sc = sc.surplusLinks.next {
			require.True(t, sc.surplus)
			listed++
		}
		require.Equal(t, surplus[pi], listed)
	}
	require.Equal(t, users, s.users)
	return totals
}

func TestMemoryMissCompleteDone(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}

	flight := NewFlight()
	lookup := testBegin(t, store, key, flight)
	require.Equal(t, OutcomeMiss, lookup.Outcome)

	testComplete(t, store, key, flight, "ok")
	<-flight.Done()
	require.Equal(t, []byte("ok"), flight.Record().Reply)

	lookup = testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
	require.Equal(t, 1, testEntries(store))
	checkInvariants(t, store)
}

func TestMemoryDuplicateJoinsRunningFlight(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	owner := NewFlight()
	testBegin(t, store, key, owner)

	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	require.Same(t, owner, lookup.Flight)

	testComplete(t, store, key, owner, "ok")
	<-lookup.Flight.Done()
	require.Equal(t, []byte("ok"), lookup.Flight.Record().Reply)
	checkInvariants(t, store)
}

// Many goroutines begin the same key at once: exactly one owns the call.
func TestMemorySingleOwnerUnderConcurrentBegin(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 9}
	var owners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lookup, err := store.Begin(key, NewFlight())
			if err == nil && lookup.Outcome == OutcomeMiss {
				owners.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), owners.Load())
}

func TestMemoryCancelInFlightCancelsOwner(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	owner := NewFlight()
	testBegin(t, store, key, owner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner.Attach(cancel)

	require.NoError(t, store.Cancel(key, testCancelled))
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	// The owner completes with the cancelled error, which is then the answer of the key.
	require.NoError(t, store.Complete(key, owner, Record{Err: testCancelled}))
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
	checkInvariants(t, store)
}

func TestMemoryCancelFlight(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}

	// Nothing runs: nothing is cancelled and nothing is recorded.
	require.False(t, store.CancelFlight(key))
	require.True(t, testIsMiss(t, store, key))

	owner := NewFlight()
	testBegin(t, store, key, owner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner.Attach(cancel)

	// The call runs: its context is cancelled, and it stays in flight until its owner completes it.
	require.True(t, store.CancelFlight(key))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	select {
	case <-owner.Done():
		t.Fatal("the flight finished before its owner completed it")
	default:
	}
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	require.Same(t, owner, lookup.Flight)
	checkInvariants(t, store)
}

func TestMemoryCancelBeforeCallLeavesCancelledRecord(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
	checkInvariants(t, store)
}

func TestMemoryCancelLeavesDoneRecord(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	testAnswer(t, store, key, "ok")

	require.NoError(t, store.Cancel(key, testCancelled))
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
	checkInvariants(t, store)
}

func TestMemoryUncachedReplyIsNotKept(t *testing.T) {
	store, _ := newTestMemory(4)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	fingerprint := Fingerprint("users.set", []byte("a"))
	flight := NewFlightWithFingerprint(fingerprint)
	testBegin(t, store, key, flight)
	testComplete(t, store, key, flight, "too large")

	// The duplicates which joined the call get the uncached marker too, as later duplicates do.
	require.True(t, flight.Record().Uncached)
	require.Empty(t, flight.Record().Reply)
	lookup := testBegin(t, store, key, NewFlightWithFingerprint(fingerprint))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
	require.Empty(t, lookup.Record.Reply)

	// The marker keeps the fingerprint, so another call with the same msg_id is still refused.
	lookup = testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	checkInvariants(t, store)
}

func TestMemoryAbandonedCallIsNotKept(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	flight := NewFlight()
	testBegin(t, store, key, flight)
	require.NoError(t, store.Complete(key, flight, Record{Abandoned: true}))
	require.True(t, flight.Record().Abandoned)

	// Nothing is kept, so a retransmit runs the call again, and the scope is released.
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.Equal(t, 0, testEntries(store))
	checkInvariants(t, store)
}

func TestMemoryAckRemovesDoneRecordsOnly(t *testing.T) {
	store, _ := newTestMemory(1024)
	scope1 := testScope("u1", "a")
	scope2 := testScope("u2", "a")
	done := Key{Scope: scope1, MsgID: 1}
	running := Key{Scope: scope1, MsgID: 2}
	other := Key{Scope: scope2, MsgID: 1}

	testAnswer(t, store, done, "ok")
	testBegin(t, store, running, NewFlight())
	testBegin(t, store, other, NewFlight())

	require.NoError(t, store.Ack(scope1, []uint64{1, 2}))
	lookup := testBegin(t, store, done, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	lookup = testBegin(t, store, running, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	// In-flight calls are not records: the acked done record was the only one kept.
	require.Equal(t, 0, testEntries(store))
	checkInvariants(t, store)
}

// With 128 entries and one-record reserves, a scope of three records keeps two: the oldest of the scope goes.
func TestMemoryEntryBound(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 2
	options.ScopeReserve = 1
	options.ScopeReserveBytes = 1024
	store := NewMemory(options)
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 3; msgID++ {
		testAnswer(t, store, Key{Scope: scope, MsgID: msgID}, "ok")
	}
	require.Equal(t, 2, testEntries(store))

	// The oldest record of the scope is the one evicted.
	require.True(t, testIsMiss(t, store, Key{Scope: scope, MsgID: 1}))
	checkInvariants(t, store)
}

func TestMemoryTTLExpiresRecords(t *testing.T) {
	store, clock := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 1}
	testAnswer(t, store, key, "ok")

	clock.advance(time.Minute - time.Nanosecond)
	_, done := testDone(t, store, key)
	require.True(t, done, "the record is an answer until its TTL has passed")

	clock.advance(time.Nanosecond)
	require.True(t, testIsMiss(t, store, key), "the record is no answer once its TTL has passed")
	require.Equal(t, 0, testEntries(store))
	checkInvariants(t, store)
}

// A duplicate msg_id with another fingerprint neither joins the call in flight, nor gets its reply.
func TestMemoryFingerprintMismatchInFlight(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	fpA := Fingerprint("users.set", []byte("a"))
	fpB := Fingerprint("users.set", []byte("b"))
	owner := NewFlightWithFingerprint(fpA)
	testBegin(t, store, key, owner)

	lookup := testBegin(t, store, key, NewFlightWithFingerprint(fpB))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Nil(t, lookup.Flight)

	lookup = testBegin(t, store, key, NewFlightWithFingerprint(fpA))
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	checkInvariants(t, store)
}

func TestMemoryFingerprintMismatchDone(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	fpA := Fingerprint("users.set", []byte("a"))
	owner := NewFlightWithFingerprint(fpA)
	testBegin(t, store, key, owner)
	require.NoError(t, store.Complete(key, owner, Record{Reply: []byte("ok")}))

	lookup := testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Empty(t, lookup.Record.Reply)

	lookup = testBegin(t, store, key, NewFlightWithFingerprint(fpA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
	checkInvariants(t, store)
}

// A cancelled record has no call, so it answers any duplicate.
func TestMemoryCancelledRecordAnswersAnyFingerprint(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup := testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
	checkInvariants(t, store)
}

// The byte budget holds: a record is kept up to its share of the reserve, 1024/4 = 256 bytes, so a 128-byte reply
// makes a 256-byte record. Eight scopes fill 8 KiB with 32 records, four per scope, which is each scope's reserve.
func TestMemoryByteBudget(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxBytes = 8 * 1024
	options.ScopeReserveBytes = 1024
	store := NewMemory(options)
	reply := make([]byte, 128)
	scopes := make([]string, 8)
	for i := range scopes {
		scopes[i] = testScope("u"+strconv.Itoa(i), "a")
	}
	for i := 0; i < 500; i++ {
		key := Key{Scope: scopes[i%len(scopes)], MsgID: uint64(i)}
		flight := NewFlight()
		testBegin(t, store, key, flight)
		require.NoError(t, store.Complete(key, flight, Record{Reply: reply}))
	}
	// 8 KiB holds 32 records of 256 bytes, and each of the eight scopes keeps its four records.
	require.LessOrEqual(t, store.Stats().Bytes, int64(8*1024))
	require.Equal(t, 32, testEntries(store))
	for _, scope := range scopes {
		require.Equal(t, 4, testRecordCount(store, scope))
	}
	checkInvariants(t, store)
}

// The per-scope cap evicts the oldest record of the scope.
func TestMemoryPerScopeCap(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxRecordsPerScope = 4
	store := NewMemory(options)
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 10; msgID++ {
		testAnswer(t, store, Key{Scope: scope, MsgID: msgID}, "ok")
	}
	require.Equal(t, 4, testEntries(store))
	require.True(t, testIsMiss(t, store, Key{Scope: scope, MsgID: 1}))
	_, done := testDone(t, store, Key{Scope: scope, MsgID: 10})
	require.True(t, done)
	checkInvariants(t, store)
}

// Cancel tombstones are records, so they count toward the cap of their scope.
func TestMemoryCancelTombstonesCountTowardCap(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxRecordsPerScope = 4
	store := NewMemory(options)
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 5; msgID++ {
		require.NoError(t, store.Cancel(Key{Scope: scope, MsgID: msgID}, testCancelled))
	}
	require.Equal(t, 4, testEntries(store))
	require.True(t, testIsMiss(t, store, Key{Scope: scope, MsgID: 1}))
	checkInvariants(t, store)
}

// The reserve holds under a flood which fills the budget. The victim's four records are within its reserve, so no
// scope evicts them. The flooder churns its own records above its reserve, and each one-record scope takes its
// room from the flooder, which is the surplus scope.
func TestMemoryReserveSurvivesFlood(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 32
	options.MaxScopesPerUser = 100
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)

	victim := testScope("v", "a")
	for msgID := uint64(1); msgID <= 4; msgID++ {
		testAnswer(t, store, Key{Scope: victim, MsgID: msgID}, "victim")
	}
	flood := testScope("x", "a")
	for msgID := uint64(1); msgID <= 1000; msgID++ {
		testAnswer(t, store, Key{Scope: flood, MsgID: msgID}, "flood")
		if msgID%100 == 0 {
			for victimMsgID := uint64(1); victimMsgID <= 4; victimMsgID++ {
				record, done := testDone(t, store, Key{Scope: victim, MsgID: victimMsgID})
				require.True(t, done)
				require.Equal(t, []byte("victim"), record.Reply)
			}
		}
	}
	// The victim holds 4 records and the flooder fills the other 28. The 972 later records of the flooder each evict
	// its own oldest, as the flooder is over its reserve while the pool is full.
	require.Equal(t, 32, testEntries(store))
	require.Equal(t, 972, observer.counts[EvictionOverReserve])
	checkInvariants(t, store)

	// Six one-record scopes of other users take 28 to 22 from the flooder, one surplus eviction each.
	for i := 0; i < 6; i++ {
		testAnswer(t, store, Key{Scope: testScope("one"+strconv.Itoa(i), "a"), MsgID: 1}, "one")
	}
	require.Equal(t, 32, testEntries(store))
	require.Equal(t, 6, observer.counts[EvictionSurplus])
	require.Equal(t, 972, observer.counts[EvictionOverReserve])

	for victimMsgID := uint64(1); victimMsgID <= 4; victimMsgID++ {
		_, done := testDone(t, store, Key{Scope: victim, MsgID: victimMsgID})
		require.True(t, done)
	}
	for msgID := uint64(1); msgID <= 978; msgID++ {
		require.True(t, testIsMiss(t, store, Key{Scope: flood, MsgID: msgID}), msgID)
	}
	for msgID := uint64(979); msgID <= 1000; msgID++ {
		_, done := testDone(t, store, Key{Scope: flood, MsgID: msgID})
		require.True(t, done, msgID)
	}
	checkInvariants(t, store)

	// The pool admits 32 / 4 = 8 scopes: the victim, the flooder and six one-record scopes. A ninth scope is refused,
	// and nothing is recorded for it.
	lookup, err := store.Begin(Key{Scope: testScope("nine", "a"), MsgID: 1}, NewFlight())
	require.Equal(t, Lookup{}, lookup)
	require.Equal(t, RefusalPool, testRefused(t, err))
	require.Equal(t, 32, testEntries(store))
	checkInvariants(t, store)
}

// A scope above its reserve, while the pool is full, evicts its own oldest record before it takes from another
// scope. Scope A has six records, B two, and the budget is eight.
func TestMemoryOverReserveEvictsOwnFirst(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 8
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	scopeA := testScope("a", "a")
	scopeB := testScope("b", "a")

	for msgID := uint64(1); msgID <= 6; msgID++ {
		testAnswer(t, store, Key{Scope: scopeA, MsgID: msgID}, "a")
	}
	testAnswer(t, store, Key{Scope: scopeB, MsgID: 1}, "b")
	testAnswer(t, store, Key{Scope: scopeB, MsgID: 2}, "b")

	// A is over its reserve and the pool is full: its own oldest goes.
	testAnswer(t, store, Key{Scope: scopeA, MsgID: 7}, "a")
	require.Equal(t, 1, observer.counts[EvictionOverReserve])
	require.True(t, testIsMiss(t, store, Key{Scope: scopeA, MsgID: 1}))
	_, done := testDone(t, store, Key{Scope: scopeB, MsgID: 1})
	require.True(t, done, "B is intact")

	// B is within its reserve: the oldest record of A, the surplus scope, goes instead.
	testAnswer(t, store, Key{Scope: scopeB, MsgID: 3}, "b")
	require.Equal(t, 1, observer.counts[EvictionSurplus])
	require.Equal(t, 0, observer.counts[EvictionUncached])
	require.True(t, testIsMiss(t, store, Key{Scope: scopeA, MsgID: 2}))
	for msgID := uint64(1); msgID <= 3; msgID++ {
		_, done = testDone(t, store, Key{Scope: scopeB, MsgID: msgID})
		require.True(t, done)
	}
	checkInvariants(t, store)
}

// Among the surplus scopes, the one which has been over its reserve the longest gives its records first. Scope A
// goes over its reserve first, then B. Scope C, within its reserve, evicts A twice, and B only once A is back at
// its reserve.
func TestMemorySurplusDrainsLongestFirst(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 12
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	scopeA := testScope("a", "a")
	scopeB := testScope("b", "a")
	scopeC := testScope("c", "a")

	for msgID := uint64(1); msgID <= 6; msgID++ {
		testAnswer(t, store, Key{Scope: scopeA, MsgID: msgID}, "a")
	}
	for msgID := uint64(1); msgID <= 5; msgID++ {
		testAnswer(t, store, Key{Scope: scopeB, MsgID: msgID}, "b")
	}
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 1}, "c")
	require.Equal(t, 12, testEntries(store))

	testAnswer(t, store, Key{Scope: scopeC, MsgID: 2}, "c")
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 3}, "c")
	require.Equal(t, 2, observer.counts[EvictionSurplus])
	require.True(t, testIsMiss(t, store, Key{Scope: scopeA, MsgID: 1}))
	require.True(t, testIsMiss(t, store, Key{Scope: scopeA, MsgID: 2}))
	_, done := testDone(t, store, Key{Scope: scopeB, MsgID: 1})
	require.True(t, done, "B is not touched while A is over its reserve")

	// A is at its reserve, so the next eviction takes B's oldest.
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 4}, "c")
	require.Equal(t, 3, observer.counts[EvictionSurplus])
	require.True(t, testIsMiss(t, store, Key{Scope: scopeB, MsgID: 1}))
	_, done = testDone(t, store, Key{Scope: scopeA, MsgID: 3})
	require.True(t, done)
	checkInvariants(t, store)
}

// A scope over its reserve which evicts its own oldest record keeps its place among the surplus scopes, so the scope
// which went over first is still drained first. Scope A goes over its reserve first, then B, and C is within its
// reserve. A takes a sixth record: its own oldest goes, and A is over its reserve still. C then evicts from the front of
// the surplus, which is A, and B keeps its five records.
func TestMemorySurplusKeepsPlaceOnChurn(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 12
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	scopeA := testScope("a", "a")
	scopeB := testScope("b", "a")
	scopeC := testScope("c", "a")

	for msgID := uint64(1); msgID <= 5; msgID++ {
		testAnswer(t, store, Key{Scope: scopeA, MsgID: msgID}, "a")
	}
	for msgID := uint64(1); msgID <= 5; msgID++ {
		testAnswer(t, store, Key{Scope: scopeB, MsgID: msgID}, "b")
	}
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 1}, "c")
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 2}, "c")
	require.Equal(t, 12, testEntries(store))

	testAnswer(t, store, Key{Scope: scopeA, MsgID: 6}, "a")
	require.Equal(t, 1, observer.counts[EvictionOverReserve])
	require.Equal(t, 5, testRecordCount(store, scopeA))

	testAnswer(t, store, Key{Scope: scopeC, MsgID: 3}, "c")
	require.Equal(t, 1, observer.counts[EvictionSurplus])
	require.True(t, testIsMiss(t, store, Key{Scope: scopeA, MsgID: 2}), "A, the scope which went over first, is drained first")
	require.Equal(t, 4, testRecordCount(store, scopeA))
	require.Equal(t, 5, testRecordCount(store, scopeB))
	checkInvariants(t, store)
}

// A reply over the share of a record of its scope is kept as uncached, so a retransmit is answered with
// result_evicted and the call is not run again. With a 2 KiB byte reserve and 4 records, a record is kept up to
// 512 bytes: a 256-byte reply makes a 512-byte record, and a 257-byte reply is over the share.
func TestMemoryRecordOverShareIsUncached(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 4096)
	options.ScopeReserveBytes = 2048
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	scope := testScope("a", "a")
	fitting := Key{Scope: scope, MsgID: 1}
	over := Key{Scope: scope, MsgID: 2}
	testAnswer(t, store, fitting, string(make([]byte, 256)))
	testAnswer(t, store, over, string(make([]byte, 257)))

	record, done := testDone(t, store, fitting)
	require.True(t, done)
	require.False(t, record.Uncached)
	require.Len(t, record.Reply, 256)

	record, done = testDone(t, store, over)
	require.True(t, done)
	require.True(t, record.Uncached)
	require.Empty(t, record.Reply)
	require.Equal(t, 1, observer.counts[EvictionUncached])
	// The fitting record counts 256 bytes of reply and overhead, the uncached one the overhead only.
	require.Equal(t, int64(256+RecordOverhead+RecordOverhead), store.Stats().Bytes)
	checkInvariants(t, store)
}

// A reply over MaxReplyBytes is kept as uncached when its call completes, and is counted as reply_size, not as uncached,
// so the reason names the limit which dropped it.
func TestMemoryReplyOverMaxIsCountedAsReplySize(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 100)
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	scope := testScope("a", "a")
	key := Key{Scope: scope, MsgID: 1}
	testAnswer(t, store, key, string(make([]byte, 101)))

	record, done := testDone(t, store, key)
	require.True(t, done)
	require.True(t, record.Uncached)
	require.Empty(t, record.Reply)
	require.Equal(t, 1, observer.counts[EvictionReplySize])
	require.Equal(t, 0, observer.counts[EvictionUncached])
	checkInvariants(t, store)
}

// A scope within its record reserve keeps all its records when the pool is full, whatever their bytes. The victim
// holds four records of 2 KiB, its byte reserve, and the flooder holds eight: the newcomer evicts the oldest of the
// flooder, the surplus scope, and not one of the victim's records.
func TestMemoryWithinReserveKeepsRecordsWhenFull(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 4096)
	options.MaxEntries = 12
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	victim := testScope("v", "a")
	flood := testScope("f", "a")
	newcomer := testScope("n", "a")

	for msgID := uint64(1); msgID <= 4; msgID++ {
		testAnswer(t, store, Key{Scope: victim, MsgID: msgID}, string(make([]byte, 1792)))
	}
	for msgID := uint64(1); msgID <= 8; msgID++ {
		testAnswer(t, store, Key{Scope: flood, MsgID: msgID}, "flood")
	}
	require.Equal(t, 12, testEntries(store))
	require.Empty(t, observer.counts[EvictionSurplus])

	testAnswer(t, store, Key{Scope: newcomer, MsgID: 1}, "new")
	require.Equal(t, 1, observer.counts[EvictionSurplus])
	require.Equal(t, 0, observer.counts[EvictionUncached])
	require.Equal(t, 4, testRecordCount(store, victim))
	for msgID := uint64(1); msgID <= 4; msgID++ {
		record, done := testDone(t, store, Key{Scope: victim, MsgID: msgID})
		require.True(t, done, msgID)
		require.Len(t, record.Reply, 1792)
	}
	require.True(t, testIsMiss(t, store, Key{Scope: flood, MsgID: 1}))
	require.Equal(t, 7, testRecordCount(store, flood))
	checkInvariants(t, store)
}

// A reply which is over the share of a shard is kept as uncached, and the budget is not exceeded.
func TestMemoryReplyOverShareIsUncached(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 65536)
	options.MaxBytes = 1200
	options.ScopeReserveBytes = 1024
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	key := Key{Scope: testScope("u1", "a"), MsgID: 1}
	flight := NewFlight()
	testBegin(t, store, key, flight)
	require.NoError(t, store.Complete(key, flight, Record{Reply: make([]byte, 2000)}))

	lookup := testBegin(t, store, key, NewFlight())
	require.True(t, lookup.Record.Uncached)
	require.Equal(t, 1, observer.counts[EvictionUncached])
	require.LessOrEqual(t, store.Stats().Bytes, int64(1200))
	checkInvariants(t, store)
}

// The anonymous pool is separate: its scopes are refused at its own limit, and an authenticated scope is still
// admitted with its records kept. The reverse holds too.
func TestMemoryAnonymousPoolIsSeparate(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 400
	options.AnonymousMaxEntries = 200
	options.AnonymousMaxBytes = 50 * 8192
	options.MaxBytes = 100 * 8192
	store := NewMemory(options)

	authenticated := testScope("u1", "session")
	testAnswer(t, store, Key{Scope: authenticated, MsgID: 1}, "auth")

	for i := 0; i < 50; i++ {
		testAnswer(t, store, Key{Scope: ScopeOf(anonymousKind, "", []byte("install-"+strconv.Itoa(i))), MsgID: 1}, "anon")
	}
	extra := ScopeOf(anonymousKind, "", []byte("install-50"))
	_, err := store.Begin(Key{Scope: extra, MsgID: 1}, NewFlight())
	require.Equal(t, RefusalPool, testRefused(t, err))

	record, done := testDone(t, store, Key{Scope: authenticated, MsgID: 1})
	require.True(t, done)
	require.Equal(t, []byte("auth"), record.Reply)
	checkInvariants(t, store)

	// The reverse: fifty authenticated users fill their pool, and an anonymous scope is still admitted.
	reversedOptions := testMemoryOptions(clock, 1024)
	reversedOptions.MaxEntries = 400
	reversedOptions.AnonymousMaxEntries = 200
	reversedOptions.MaxBytes = 100 * 8192
	reversedOptions.AnonymousMaxBytes = 50 * 8192
	reversedOptions.MaxScopesPerUser = 100
	reversed := NewMemory(reversedOptions)
	for i := 0; i < 50; i++ {
		testAnswer(t, reversed, Key{Scope: testScope("user"+strconv.Itoa(i), "a"), MsgID: 1}, "auth")
	}
	_, err = reversed.Begin(Key{Scope: testScope("user50", "a"), MsgID: 1}, NewFlight())
	require.Equal(t, RefusalPool, testRefused(t, err))
	testAnswer(t, reversed, Key{Scope: ScopeOf(anonymousKind, "", []byte("install")), MsgID: 1}, "anon")
	checkInvariants(t, reversed)
}

// A user holds MaxScopesPerUser scopes at most. The next scope of the user is refused, another user is admitted,
// and the slot frees when a scope expires or is acked.
func TestMemoryPerUserScopeCap(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxScopesPerUser = 3
	store := NewMemory(options)

	scopes := []string{testScope("u", "1"), testScope("u", "2"), testScope("u", "3")}
	for _, scope := range scopes {
		testAnswer(t, store, Key{Scope: scope, MsgID: 1}, "ok")
	}
	fourth := testScope("u", "4")
	_, err := store.Begin(Key{Scope: fourth, MsgID: 1}, NewFlight())
	require.Equal(t, RefusalUser, testRefused(t, err))

	testAnswer(t, store, Key{Scope: testScope("other", "1"), MsgID: 1}, "ok")
	checkInvariants(t, store)

	// Acking the record of one scope frees its slot.
	require.NoError(t, store.Ack(scopes[0], []uint64{1}))
	testAnswer(t, store, Key{Scope: fourth, MsgID: 1}, "ok")
	checkInvariants(t, store)

	// A scope which expires frees its slot too.
	clock.advance(time.Minute)
	testAnswer(t, store, Key{Scope: testScope("u", "5"), MsgID: 1}, "ok")
	checkInvariants(t, store)
}

// A cancel for a scope which the store does not admit records nothing.
func TestMemoryCancelTombstoneRefusedWhenFull(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 8
	store := NewMemory(options)
	testAnswer(t, store, Key{Scope: testScope("a", "a"), MsgID: 1}, "ok")
	testAnswer(t, store, Key{Scope: testScope("b", "a"), MsgID: 1}, "ok")
	require.Equal(t, 2, testEntries(store))

	key := Key{Scope: testScope("c", "a"), MsgID: 1}
	err := store.Cancel(key, testCancelled)
	require.Equal(t, RefusalPool, testRefused(t, err))
	require.Equal(t, 2, testEntries(store))
	_, err = store.Begin(key, NewFlight())
	require.Equal(t, RefusalPool, testRefused(t, err))

	// A user at its scope cap is refused in the same way.
	options.MaxScopesPerUser = 1
	options.MaxEntries = 1000
	store = NewMemory(options)
	testAnswer(t, store, Key{Scope: testScope("u", "1"), MsgID: 1}, "ok")
	err = store.Cancel(Key{Scope: testScope("u", "2"), MsgID: 1}, testCancelled)
	require.Equal(t, RefusalUser, testRefused(t, err))
	require.Equal(t, 1, testEntries(store))
	checkInvariants(t, store)
}

// A scope with only a call in flight is admitted, so a competing new scope is refused. Abandoning the call
// releases the scope.
func TestMemoryFlightHoldsAdmission(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 8
	store := NewMemory(options)
	scopeA := testScope("a", "a")
	scopeB := testScope("b", "a")
	scopeC := testScope("c", "a")

	flightA := NewFlight()
	testBegin(t, store, Key{Scope: scopeA, MsgID: 1}, flightA)
	testBegin(t, store, Key{Scope: scopeB, MsgID: 1}, NewFlight())
	_, err := store.Begin(Key{Scope: scopeC, MsgID: 1}, NewFlight())
	require.Equal(t, RefusalPool, testRefused(t, err))
	checkInvariants(t, store)

	require.NoError(t, store.Complete(Key{Scope: scopeA, MsgID: 1}, flightA, Record{Abandoned: true}))
	testAnswer(t, store, Key{Scope: scopeC, MsgID: 1}, "ok")
	checkInvariants(t, store)
}

// The expired records of a shard are removed a few at a time, so that one access does not walk all of them.
func TestMemorySweepIsCapped(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 1000
	options.MaxScopesPerUser = 1000
	store := NewMemory(options)
	for i := 0; i < 200; i++ {
		testAnswer(t, store, Key{Scope: testScope("u"+strconv.Itoa(i/4), "a"), MsgID: uint64(i)}, "ok")
	}
	require.Equal(t, 200, testEntries(store))

	// Every record has expired. One Begin removes at most sweepLimit of them, and it is the Begin that removes them.
	clock.advance(time.Minute)
	fresh := Key{Scope: testScope("fresh", "a"), MsgID: 1}
	flight := NewFlight()
	require.Equal(t, OutcomeMiss, testBegin(t, store, fresh, flight).Outcome)
	require.Equal(t, 200-sweepLimit, testEntries(store))

	// The next access removes the next batch, and no more.
	require.NoError(t, store.Complete(fresh, flight, Record{Abandoned: true}))
	require.True(t, testIsMiss(t, store, Key{Scope: testScope("fresh", "a"), MsgID: 2}))
	require.Equal(t, 200-2*sweepLimit, testEntries(store))
	checkInvariants(t, store)
}

// A new scope is refused by a pool at its scope limit only when the expired records of the shard cannot release a
// scope: the admission sweeps them, so the scope is admitted at once. The records here outnumber one sweep.
func TestMemoryAdmissionSweepsExpiredScopes(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 400
	options.ScopeReserve = 100
	options.MaxRecordsPerScope = 100
	options.ScopeReserveBytes = 1 << 20
	store := NewMemory(options)
	for i := 0; i < 4; i++ {
		scope := testScope("u"+strconv.Itoa(i), "a")
		for msgID := uint64(1); msgID <= 100; msgID++ {
			testAnswer(t, store, Key{Scope: scope, MsgID: msgID}, "ok")
		}
	}
	require.Equal(t, int64(4), store.Stats().Authenticated.Scopes)

	clock.advance(time.Minute)
	fresh := Key{Scope: testScope("fresh", "a"), MsgID: 1}
	testAnswer(t, store, fresh, "new")
	require.Equal(t, int64(4), store.Stats().Authenticated.Scopes)
	checkInvariants(t, store)
}

// RecordOverhead counts the storedRecord struct of a record, so the bytes of the budget cover the struct.
func TestRecordOverheadCoversStoredRecord(t *testing.T) {
	require.LessOrEqual(t, int(unsafe.Sizeof(storedRecord{})), RecordOverhead)
}

// The insert loop frees the expired records of a full pool before it evicts a live one.
func TestMemoryInsertFreesExpiredBeforeLive(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxEntries = 8
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	old := testScope("old", "a")
	for msgID := uint64(1); msgID <= 8; msgID++ {
		testAnswer(t, store, Key{Scope: old, MsgID: msgID}, "old")
	}

	// The call of the new scope starts before the records expire, and completes after.
	fresh := testScope("fresh", "a")
	key := Key{Scope: fresh, MsgID: 1}
	flight := NewFlight()
	require.Equal(t, OutcomeMiss, testBegin(t, store, key, flight).Outcome)
	clock.advance(time.Minute)
	require.NoError(t, store.Complete(key, flight, Record{Reply: []byte("new")}))

	require.Empty(t, observer.counts, "expired records make room without an eviction")
	require.Equal(t, 8, testEntries(store), "seven expired records and the new one")
	record, done := testDone(t, store, key)
	require.True(t, done)
	require.Equal(t, []byte("new"), record.Reply)
	checkInvariants(t, store)
}

// Completing a flight a second time is ignored: the first record is kept, and the counters count one record.
func TestMemoryCompleteTwiceKeepsFirstRecord(t *testing.T) {
	store, _ := newTestMemory(1024)
	key := Key{Scope: testScope("u1", "a"), MsgID: 1}
	flight := NewFlight()
	testBegin(t, store, key, flight)
	require.NoError(t, store.Complete(key, flight, Record{Reply: []byte("first")}))
	require.NoError(t, store.Complete(key, flight, Record{Reply: []byte("second")}))

	require.Equal(t, 1, testEntries(store))
	require.Equal(t, int64(RecordOverhead+len("first")), store.Stats().Bytes)
	record, _ := testDone(t, store, key)
	require.Equal(t, []byte("first"), record.Reply)
	checkInvariants(t, store)
}

// The records of a mixed load keep the budgets, the counts and the structures at every step.
func TestMemoryBudgetsHoldUnderMixedLoad(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 4096)
	options.Shards = 4
	// Sixty entries are fifteen per shard, so each shard fills whatever the placement of its scopes.
	options.MaxEntries = 60
	options.MaxBytes = 200 * 1024
	options.ScopeReserveBytes = 1024
	options.MaxScopesPerUser = 8
	observer := newCountingObserver()
	options.Observer = observer
	store := NewMemory(options)
	random := rand.New(rand.NewPCG(1, 2))
	refusals := 0

	const users = 40
	const sessions = 300
	scopes := make([]string, sessions)
	for i := range scopes {
		scopes[i] = testScope("u"+strconv.Itoa(i%users), strconv.Itoa(i))
	}
	var issued []Key
	for op := 1; op <= 20000; op++ {
		scope := scopes[random.IntN(len(scopes))]
		// Most calls go to eight hot scopes, which get admitted and go over their reserve, so the budgets fill.
		if random.IntN(10) < 7 {
			scope = scopes[random.IntN(8)]
		}
		switch random.IntN(10) {
		case 0, 1, 2, 3, 4, 5:
			key := Key{Scope: scope, MsgID: uint64(random.IntN(64))}
			flight := NewFlight()
			lookup, err := store.Begin(key, flight)
			if err != nil {
				refusals++
			} else if lookup.Outcome == OutcomeMiss {
				reply := make([]byte, random.IntN(2000))
				require.NoError(t, store.Complete(key, flight, Record{Reply: reply}))
				issued = append(issued, key)
			}
		case 6:
			// A cancel of a scope which is not admitted is refused, and records nothing.
			err := store.Cancel(Key{Scope: scope, MsgID: uint64(random.IntN(64))}, testCancelled)
			if err != nil {
				var refused *RefusedError
				require.ErrorAs(t, err, &refused)
			}
		case 7:
			if len(issued) > 0 {
				key := issued[random.IntN(len(issued))]
				require.NoError(t, store.Ack(key.Scope, []uint64{key.MsgID}))
			}
		case 8:
			// Records live for the TTL, about 1200 operations here, so the budgets fill before they expire.
			clock.advance(time.Duration(random.IntN(1000)) * time.Millisecond)
		default:
			require.NoError(t, store.Ack(scope, []uint64{uint64(random.IntN(64))}))
		}
		if op%1000 == 0 {
			stats := store.Stats()
			require.LessOrEqual(t, testEntries(store), options.MaxEntries)
			require.LessOrEqual(t, stats.Bytes, options.MaxBytes)
			checkInvariants(t, store)
		}
	}
	// The load reaches both limits, so the checks above are not vacuous.
	require.Positive(t, refusals)
	require.Positive(t, observer.counts[EvictionSurplus]+observer.counts[EvictionOverReserve]+observer.counts[EvictionScopeCap])
}

// The store admits a scope only up to the limit its pool allows, and the budgets stay whole with shards.
func TestMemoryPoolScopeLimitHoldsWithShards(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.Shards = 8
	options.MaxEntries = 64
	store := NewMemory(options)
	admitted := 0
	for i := 0; i < 200; i++ {
		_, err := store.Begin(Key{Scope: testScope("u"+strconv.Itoa(i), "a"), MsgID: 1}, NewFlight())
		if err == nil {
			admitted++
		}
	}
	require.LessOrEqual(t, admitted, options.MaxEntries/options.ScopeReserve)
	require.Equal(t, admitted, int(store.Stats().Authenticated.Scopes))
	checkInvariants(t, store)
}
