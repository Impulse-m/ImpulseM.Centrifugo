package rpcdedup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

var testCancelled *centrifuge.Error = &centrifuge.Error{Code: 107, Message: "cancelled"}

// testBegin runs Begin for key and fails the test on an error.
func testBegin(t *testing.T, store Store, key Key, flight *Flight) Lookup {
	t.Helper()
	var lookup Lookup
	var err error
	lookup, err = store.Begin(key, flight)
	require.NoError(t, err)
	return lookup
}

// testComplete keeps the answer of flight for key with the reply data.
func testComplete(t *testing.T, store Store, key Key, flight *Flight, data string) {
	t.Helper()
	require.NoError(t, store.Complete(key, flight, Record{Reply: []byte(data)}))
}

func TestMemoryMissCompleteDone(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}

	flight :=NewFlight()
	lookup :=testBegin(t, store, key, flight)
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
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	owner :=NewFlight()
	testBegin(t, store, key, owner)

	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	require.Same(t, owner, lookup.Flight)

	testComplete(t, store, key, owner, "ok")
	<-lookup.Flight.Done()
	require.Equal(t, []byte("ok"), lookup.Flight.Record().Reply)
}

// Many goroutines begin the same key at once: exactly one owns the call.
func TestMemorySingleOwnerUnderConcurrentBegin(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 9}
	var owners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lookup Lookup
			var err error
			lookup, err = store.Begin(key, NewFlight())
			if err == nil && lookup.Outcome == OutcomeMiss {
				owners.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), owners.Load())
}

func TestMemoryCancelInFlightCancelsOwner(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	owner :=NewFlight()
	testBegin(t, store, key, owner)
	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	owner.Attach(cancel)

	require.NoError(t, store.Cancel(key, testCancelled))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.True(t, owner.Cancelled())

	// The owner completes with the cancelled error, which is then the answer of the key.
	require.NoError(t, store.Complete(key, owner, Record{Err: testCancelled}))
	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}

func TestMemoryCancelBeforeCallLeavesCancelledRecord(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}

func TestMemoryCancelLeavesDoneRecord(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	flight :=NewFlight()
	testBegin(t, store, key, flight)
	testComplete(t, store, key, flight, "ok")

	require.NoError(t, store.Cancel(key, testCancelled))
	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

func TestMemoryUncachedReplyIsNotKept(t *testing.T) {
	store :=NewMemory(10, time.Minute, 4)
	key :=Key{Scope: "s:1", MsgID: 7}
	flight :=NewFlight()
	testBegin(t, store, key, flight)
	testComplete(t, store, key, flight, "too large")

	// The duplicates which joined the call got the reply, later duplicates get the uncached marker.
	require.Equal(t, []byte("too large"), flight.Record().Reply)
	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
	require.Empty(t, lookup.Record.Reply)
}

func TestMemoryAbandonedCallIsNotKept(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	flight :=NewFlight()
	testBegin(t, store, key, flight)
	require.NoError(t, store.Complete(key, flight, Record{Abandoned: true}))
	require.True(t, flight.Record().Abandoned)

	// Nothing is kept, so a retransmit runs the call again.
	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.Equal(t, 0, store.Len())
}

func TestMemoryAckRemovesDoneRecordsOnly(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	done :=Key{Scope: "s:1", MsgID: 1}
	running :=Key{Scope: "s:1", MsgID: 2}
	other :=Key{Scope: "s:2", MsgID: 1}

	flight :=NewFlight()
	testBegin(t, store, done, flight)
	testComplete(t, store, done, flight, "ok")
	testBegin(t, store, running, NewFlight())
	testBegin(t, store, other, NewFlight())

	require.NoError(t, store.Ack("s:1", []uint64{1, 2}))
	lookup :=testBegin(t, store, done, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	lookup = testBegin(t, store, running, NewFlight())
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
	// In-flight calls are not records: the acked done record was the only one kept.
	require.Equal(t, 0, store.Len())
}

func TestMemoryLRUBound(t *testing.T) {
	store :=NewMemory(2, time.Minute, 1024)
	for msgID := uint64(1); msgID <= 3; msgID++ {
		key :=Key{Scope: "s:1", MsgID: msgID}
		flight :=NewFlight()
		testBegin(t, store, key, flight)
		testComplete(t, store, key, flight, "ok")
	}
	require.Equal(t, 2, store.Len())

	// The least recently used record is the one evicted.
	lookup :=testBegin(t, store, Key{Scope: "s:1", MsgID: 1}, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

func TestMemoryTTLExpiresRecords(t *testing.T) {
	store :=NewMemory(10, 20*time.Millisecond, 1024)
	key :=Key{Scope: "s:1", MsgID: 1}
	flight :=NewFlight()
	testBegin(t, store, key, flight)
	testComplete(t, store, key, flight, "ok")

	time.Sleep(60 * time.Millisecond)
	lookup :=testBegin(t, store, key, NewFlight())
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

// A duplicate msg_id with another fingerprint neither joins the call in flight, nor gets its reply.
func TestMemoryFingerprintMismatchInFlight(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	fpA :=Fingerprint("users.set", []byte("a"))
	fpB :=Fingerprint("users.set", []byte("b"))
	owner :=NewFlightWithFingerprint(fpA)
	testBegin(t, store, key, owner)

	lookup :=testBegin(t, store, key, NewFlightWithFingerprint(fpB))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Nil(t, lookup.Flight)

	lookup = testBegin(t, store, key, NewFlightWithFingerprint(fpA))
	require.Equal(t, OutcomeInFlight, lookup.Outcome)
}

func TestMemoryFingerprintMismatchDone(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	fpA :=Fingerprint("users.set", []byte("a"))
	owner :=NewFlightWithFingerprint(fpA)
	testBegin(t, store, key, owner)
	require.NoError(t, store.Complete(key, owner, Record{Reply: []byte("ok")}))

	lookup :=testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Empty(t, lookup.Record.Reply)

	lookup = testBegin(t, store, key, NewFlightWithFingerprint(fpA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

// A cancelled record has no call, so it answers any duplicate.
func TestMemoryCancelledRecordAnswersAnyFingerprint(t *testing.T) {
	store :=NewMemory(10, time.Minute, 1024)
	key :=Key{Scope: "s:1", MsgID: 7}
	require.NoError(t, store.Cancel(key, testCancelled))

	lookup :=testBegin(t, store, key, NewFlightWithFingerprint(Fingerprint("users.set", []byte("b"))))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}
