package rpcdedup

import (
	"testing"
	"time"

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
