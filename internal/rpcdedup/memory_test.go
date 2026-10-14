package rpcdedup

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

var testCancelled *centrifuge.Error = &centrifuge.Error{Code: 107, Message: "cancelled"}

// testMemoryOptions returns options with budgets large enough for the store tests, and a reply limit of maxReply.
func testMemoryOptions(maxReply int) MemoryOptions {
	return MemoryOptions{
		MaxEntries:         1000,
		MaxBytes:           1 << 30,
		MaxRecordsPerScope: 1024,
		TTL:                time.Minute,
		MaxReplyBytes:      maxReply,
	}
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

func TestMemoryMissCompleteDone(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}

	flight := NewFlight()
	lookup := testBegin(t, store, key, flight)
	require.Equal(t, OutcomeMiss, lookup.Outcome)

	testComplete(t, store, key, flight, "ok")
	<-flight.Done()
	require.Equal(t, []byte("ok"), flight.Record().Reply)

	lookup = testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
	require.Equal(t, 1, store.Len())
}

func TestMemoryDuplicateJoinsRunningFlight(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	owner := NewFlight()
	testBegin(t, store, key, owner)

	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	require.Same(t, owner, lookup.Flight)

	testComplete(t, store, key, owner, "ok")
	<-lookup.Flight.Done()
	require.Equal(t, []byte("ok"), lookup.Flight.Record().Reply)
}

// Many goroutines begin the same key at once: exactly one owns the call.
func TestMemorySingleOwnerUnderConcurrentBegin(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 9}
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
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
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
}

func TestMemoryCancelBeforeCallLeavesCancelledRecord(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}

func TestMemoryCancelLeavesDoneRecord(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	testAnswer(t, store, key, "ok")

	require.NoError(t, store.Cancel(key, testCancelled))
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

func TestMemoryUncachedReplyIsNotKept(t *testing.T) {
	store := NewMemory(testMemoryOptions(4))
	key := Key{Scope: "s:1", MsgID: 7}
	fingerprint := Fingerprint("users.set", []byte("a"))
	flight := NewFlightWithFingerprint(fingerprint)
	testBegin(t, store, key, flight)
	testComplete(t, store, key, flight, "too large")

	// The duplicates which joined the call got the reply, later duplicates get the uncached marker.
	require.Equal(t, []byte("too large"), flight.Record().Reply)
	lookup := testBegin(t, store, key, NewFlightWithFingerprint(fingerprint))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
	require.Empty(t, lookup.Record.Reply)

	// The marker keeps the fingerprint, so another call with the same msg_id is still refused.
	lookup = testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
}

func TestMemoryAbandonedCallIsNotKept(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	flight := NewFlight()
	testBegin(t, store, key, flight)
	require.NoError(t, store.Complete(key, flight, Record{Abandoned: true}))
	require.True(t, flight.Record().Abandoned)

	// Nothing is kept, so a retransmit runs the call again.
	lookup := testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.Equal(t, 0, store.Len())
}

func TestMemoryAckRemovesDoneRecordsOnly(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	done := Key{Scope: "s:1", MsgID: 1}
	running := Key{Scope: "s:1", MsgID: 2}
	other := Key{Scope: "s:2", MsgID: 1}

	testAnswer(t, store, done, "ok")
	testBegin(t, store, running, NewFlight())
	testBegin(t, store, other, NewFlight())

	require.NoError(t, store.Ack("s:1", []uint64{1, 2}))
	lookup := testBegin(t, store, done, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	lookup = testBegin(t, store, running, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	// In-flight calls are not records: the acked done record was the only one kept.
	require.Equal(t, 0, store.Len())
}

// Entries are shared out over the shards, so 128 entries leave two per shard. The keys share one scope, so
// one shard.
func TestMemoryEntryBound(t *testing.T) {
	options := testMemoryOptions(1024)
	options.MaxEntries = 128
	store := NewMemory(options)
	for msgID := uint64(1); msgID <= 3; msgID++ {
		testAnswer(t, store, Key{Scope: "s:1", MsgID: msgID}, "ok")
	}
	require.Equal(t, 2, store.Len())

	// The oldest record of the scope is the one evicted.
	lookup := testBegin(t, store, Key{Scope: "s:1", MsgID: 1}, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

func TestMemoryTTLExpiresRecords(t *testing.T) {
	options := testMemoryOptions(1024)
	options.TTL = 20 * time.Millisecond
	store := NewMemory(options)
	key := Key{Scope: "s:1", MsgID: 1}
	testAnswer(t, store, key, "ok")

	// The record is no answer once its TTL has passed.
	require.Eventually(t, func() bool {
		lookup := testBegin(t, store, key, NewFlight())
		return lookup.Outcome == OutcomeMiss
	}, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return store.Len() == 0
	}, 2*time.Second, time.Millisecond)
}

// A duplicate msg_id with another fingerprint neither joins the call in flight, nor gets its reply.
func TestMemoryFingerprintMismatchInFlight(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	fpA := Fingerprint("users.set", []byte("a"))
	fpB := Fingerprint("users.set", []byte("b"))
	owner := NewFlightWithFingerprint(fpA)
	testBegin(t, store, key, owner)

	lookup := testBegin(t, store, key, NewFlightWithFingerprint(fpB))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Nil(t, lookup.Flight)

	lookup = testBegin(t, store, key, NewFlightWithFingerprint(fpA))
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
}

func TestMemoryFingerprintMismatchDone(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
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
}

// A cancelled record has no call, so it answers any duplicate.
func TestMemoryCancelledRecordAnswersAnyFingerprint(t *testing.T) {
	store := NewMemory(testMemoryOptions(1024))
	key := Key{Scope: "s:1", MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup := testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}

// The byte budget is shared out over the shards: 64 shards with 1200 bytes each, and records of 528 bytes, so
// the store can keep two records per shard, not one per call.
func TestMemoryByteBudget(t *testing.T) {
	options := testMemoryOptions(1024)
	options.MaxBytes = 64 * 1200
	store := NewMemory(options)
	reply := make([]byte, 400)
	for i := 0; i < 500; i++ {
		key := Key{Scope: "s:" + strconv.Itoa(i), MsgID: 1}
		flight := NewFlight()
		testBegin(t, store, key, flight)
		require.NoError(t, store.Complete(key, flight, Record{Reply: reply}))
	}
	require.LessOrEqual(t, store.Bytes(), int64(64*1200))
	require.Less(t, store.Len(), 500)
}

// The per-scope cap evicts the oldest record of the scope.
func TestMemoryPerScopeCap(t *testing.T) {
	options := testMemoryOptions(1024)
	options.MaxRecordsPerScope = 3
	store := NewMemory(options)
	for msgID := uint64(1); msgID <= 10; msgID++ {
		testAnswer(t, store, Key{Scope: "s:1", MsgID: msgID}, "ok")
	}
	require.Equal(t, 3, store.Len())
	require.Equal(t, OutcomeMiss, testBegin(t, store, Key{Scope: "s:1", MsgID: 1}, NewFlight()).Outcome)
	require.Equal(t, OutcomeDone, testBegin(t, store, Key{Scope: "s:1", MsgID: 10}, NewFlight()).Outcome)
}

// One scope which floods the store does not evict the records of another scope, when the budgets are not
// reached: the flood is held to its own cap.
func TestMemoryFloodDoesNotEvictOtherScope(t *testing.T) {
	options := testMemoryOptions(1024)
	options.MaxRecordsPerScope = 4
	store := NewMemory(options)
	victim := Key{Scope: "s:victim", MsgID: 1}
	testAnswer(t, store, victim, "kept")

	for msgID := uint64(1); msgID <= 1000; msgID++ {
		testAnswer(t, store, Key{Scope: "s:flood", MsgID: msgID}, "flood")
	}
	lookup := testBegin(t, store, victim, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("kept"), lookup.Record.Reply)
	require.LessOrEqual(t, store.Len(), 5)
}

// Cancel tombstones are records, so they count toward the cap of their scope.
func TestMemoryCancelTombstonesCountTowardCap(t *testing.T) {
	options := testMemoryOptions(1024)
	options.MaxRecordsPerScope = 2
	store := NewMemory(options)
	for msgID := uint64(1); msgID <= 3; msgID++ {
		require.NoError(t, store.Cancel(Key{Scope: "s:1", MsgID: msgID}, testCancelled))
	}
	require.Equal(t, 2, store.Len())
	require.Equal(t, OutcomeMiss, testBegin(t, store, Key{Scope: "s:1", MsgID: 1}, NewFlight()).Outcome)
}
