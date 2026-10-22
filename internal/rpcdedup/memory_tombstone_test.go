package rpcdedup

import (
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// An ack keeps the tombstone of a cancelled call, which answers its retransmit.
func TestMemoryAckKeepsCancelledTombstone(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	store := NewMemory(testMemoryOptions(clock, 2048))
	scope := testScope("a", "a")
	key := Key{Scope: scope, MsgID: 1}

	require.NoError(t, store.Cancel(key, testCancelled))
	require.NoError(t, store.Ack(scope, []uint64{1}))

	require.Equal(t, 1, int(store.Stats().Entries), "the tombstone is kept")
}

// An ack keeps a tombstone which is downgraded to uncached for its size: the downgrade keeps the tombstone flag.
func TestMemoryAckKeepsDowngradedTombstone(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	store := NewMemory(testMemoryOptions(clock, 1<<20))
	scope := testScope("a", "a")
	key := Key{Scope: scope, MsgID: 1}
	failure := &centrifuge.Error{Code: 107, Message: strings.Repeat("x", 4096)}

	require.NoError(t, store.Tombstone(key, failure))
	require.NoError(t, store.Ack(scope, []uint64{1}))

	require.Equal(t, 1, int(store.Stats().Entries), "the downgraded tombstone is kept")
}

// An ack keeps a tombstone which is over the reply limit when it is kept: the replacement keeps the tombstone flag, so an
// ack does not remove the cancel which answers a retransmit on a node without the shared tier.
func TestMemoryAckKeepsOversizeTombstone(t *testing.T) {
	store, _ := newTestMemory(512)
	scope := testScope("u1", "a")
	key := Key{Scope: scope, MsgID: 7}
	flight := NewFlight()
	testBegin(t, store, key, flight)
	failure := &centrifuge.Error{Code: 107, Message: strings.Repeat("e", 600)}

	require.NoError(t, store.Complete(key, flight, Record{Err: failure, tombstone: true}))
	require.NoError(t, store.Ack(scope, []uint64{7}))

	require.Equal(t, 1, testEntries(store), "the oversize tombstone is kept")
	checkInvariants(t, store)
}

// An ack removes a done record, even one with an error and no fingerprint: the shared tier removes it too, since only
// its tombstones keep their state.
func TestMemoryAckRemovesDoneRecordWithError(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	store := NewMemory(testMemoryOptions(clock, 2048))
	scope := testScope("a", "a")
	key := Key{Scope: scope, MsgID: 1}
	flight := NewFlightWithFingerprint([32]byte{})

	require.Equal(t, OutcomeMiss, testBegin(t, store, key, flight).Outcome)
	require.NoError(t, store.Complete(key, flight, Record{Err: testCancelled}))
	require.NoError(t, store.Ack(scope, []uint64{1}))

	require.Zero(t, store.Stats().Entries, "the done record is removed")
}

// A call which is cancelled while it runs keeps no tombstone: its answer is a done record with the cancelled error, so an
// ack removes it, and a retransmit after the ack runs the call again. The README says so; a conforming client does not
// retransmit an acked msg_id.
func TestMemoryAckOfRunningCancelIsNotKept(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	store := NewMemory(testMemoryOptions(clock, 2048))
	scope := testScope("a", "a")
	key := Key{Scope: scope, MsgID: 1}
	flight := NewFlightWithFingerprint([32]byte{})

	require.Equal(t, OutcomeMiss, testBegin(t, store, key, flight).Outcome)
	require.NoError(t, store.Cancel(key, testCancelled))
	require.NoError(t, store.Complete(key, flight, Record{Err: testCancelled}))
	require.NoError(t, store.Ack(scope, []uint64{1}))

	require.Zero(t, store.Stats().Entries, "the cancelled done record is removed by the ack")
	require.Equal(t, OutcomeMiss, testBegin(t, store, key, NewFlightWithFingerprint([32]byte{})).Outcome, "a retransmit after the ack runs the call again")
}
