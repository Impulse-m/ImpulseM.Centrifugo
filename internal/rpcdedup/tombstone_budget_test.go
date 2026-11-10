package rpcdedup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTombstoneBudgetMemory returns a node-local store which keeps maxScope records per scope, and its clock.
func newTombstoneBudgetMemory(maxScope int) (*Memory, *testClock) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxRecordsPerScope = maxScope
	return NewMemory(options), clock
}

// newTombstoneBudgetShared returns a shared store over a node-local store which keeps maxScope records per scope. It
// returns the store, its node-local store, its backend and the clock.
func newTombstoneBudgetShared(t *testing.T, maxScope int) (*Shared, *Memory, *fakeBackend, *testClock) {
	t.Helper()
	local, clock := newTombstoneBudgetMemory(maxScope)
	backend := newFakeBackend()
	shared := NewShared(SharedOptions{
		Local:         local,
		Backend:       backend,
		NodeID:        "node-a",
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
	})
	return shared, local, backend, clock
}

// A scope which has written its tombstones of the window is refused a cancel of a new msg_id, and nothing is written to
// the shared tier for the refused cancel.
func TestTombstoneBudgetRefusesCancelOverBudget(t *testing.T) {
	shared, local, backend, _ := newTombstoneBudgetShared(t, 2)
	scope := testScope("u1", "a")
	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 1}, testCancelled))
	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 2}, testCancelled))

	refusedKey := Key{Scope: scope, MsgID: 3}
	err := shared.Cancel(refusedKey, testCancelled)
	require.Equal(t, RefusalTombstones, testRefused(t, err))
	require.Equal(t, 2, countCalls(backend.callLog(), "cancel"), "the refused cancel writes nothing to the shared tier")
	_, found := backend.entry(refusedKey)
	require.False(t, found)
	require.Equal(t, 2, testEntries(local))
	checkInvariants(t, local)
}

// The budget is per scope: another session of the same user keeps its own tombstones.
func TestTombstoneBudgetIsPerScope(t *testing.T) {
	shared, _, backend, _ := newTombstoneBudgetShared(t, 1)
	first := testScope("u1", "a")
	require.NoError(t, shared.Cancel(Key{Scope: first, MsgID: 1}, testCancelled))
	err := shared.Cancel(Key{Scope: first, MsgID: 2}, testCancelled)
	require.Equal(t, RefusalTombstones, testRefused(t, err))

	require.NoError(t, shared.Cancel(Key{Scope: testScope("u1", "b"), MsgID: 1}, testCancelled))
	require.Equal(t, 2, countCalls(backend.callLog(), "cancel"))
}

// A new TTL window gives the scope a new budget: the window ends one TTL after its first tombstone, and the budget
// of the scope is new once it has ended.
func TestTombstoneBudgetRestartsInNewWindow(t *testing.T) {
	shared, local, _, clock := newTombstoneBudgetShared(t, 2)
	scope := testScope("u1", "a")
	cancelMsg := func(msgID uint64) error {
		return shared.Cancel(Key{Scope: scope, MsgID: msgID}, testCancelled)
	}
	require.NoError(t, cancelMsg(1))
	require.NoError(t, cancelMsg(2))
	require.Equal(t, RefusalTombstones, testRefused(t, cancelMsg(3)))

	clock.advance(time.Minute - time.Second)
	require.Equal(t, RefusalTombstones, testRefused(t, cancelMsg(3)), "the window has not ended")

	clock.advance(time.Second)
	require.NoError(t, cancelMsg(3), "the window has ended, so the budget is new")
	require.NoError(t, cancelMsg(4))
	require.Equal(t, RefusalTombstones, testRefused(t, cancelMsg(5)))
	checkInvariants(t, local)
}

// A cancel of a call which runs on this node is cancelled over the budget, and makes no call of the shared tier.
func TestTombstoneBudgetCancelOfCallInFlightWorksOverBudget(t *testing.T) {
	shared, _, backend, _ := newTombstoneBudgetShared(t, 1)
	scope := testScope("u1", "a")
	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 1}, testCancelled))
	err := shared.Cancel(Key{Scope: scope, MsgID: 2}, testCancelled)
	require.Equal(t, RefusalTombstones, testRefused(t, err))

	key := Key{Scope: scope, MsgID: 3}
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, beginErr := shared.Begin(key, owner)
	require.NoError(t, beginErr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner.Attach(cancel)

	callsBefore := len(backend.callLog())
	require.NoError(t, shared.Cancel(key, testCancelled))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Len(t, backend.callLog(), callsBefore, "the call runs here, so the shared tier is not called")
}

// A cancel which finds its call in flight on this node writes no tombstone, so it charges nothing to the budget.
func TestTombstoneBudgetFlightCancelDoesNotCharge(t *testing.T) {
	shared, _, _, _ := newTombstoneBudgetShared(t, 2)
	scope := testScope("u1", "a")
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := shared.Begin(Key{Scope: scope, MsgID: 1}, owner)
	require.NoError(t, err)
	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 1}, testCancelled))

	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 2}, testCancelled))
	require.NoError(t, shared.Cancel(Key{Scope: scope, MsgID: 3}, testCancelled), "the flight cancel charged nothing, so the window has room")
	require.Equal(t, RefusalTombstones, testRefused(t, shared.Cancel(Key{Scope: scope, MsgID: 4}, testCancelled)))
}

// CheckTombstone admits no scope and charges nothing, however often it is called.
func TestTombstoneBudgetCheckRecordsNothing(t *testing.T) {
	local, _ := newTombstoneBudgetMemory(1)
	scope := testScope("u1", "a")
	for range 3 {
		require.NoError(t, local.CheckTombstone(scope))
	}
	require.Zero(t, testRecordCount(local, scope))
	require.Zero(t, local.Stats().Authenticated.Scopes)

	require.NoError(t, local.Tombstone(Key{Scope: scope, MsgID: 1}, testCancelled))
	require.Equal(t, RefusalTombstones, testRefused(t, local.CheckTombstone(scope)))
	checkInvariants(t, local)
}

// The node-local store alone never refuses a cancel for the budget, since its own cap already bounds its records.
func TestTombstoneBudgetNodeLocalCancelIsNotRefused(t *testing.T) {
	local, _ := newTombstoneBudgetMemory(1)
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 3; msgID++ {
		require.NoError(t, local.Cancel(Key{Scope: scope, MsgID: msgID}, testCancelled))
	}
	require.Equal(t, 1, testRecordCount(local, scope), "the scope cap keeps one record")
	checkInvariants(t, local)
}

// A cancel which finds a live record keeps the record and charges nothing, while a tombstone which replaces a done
// record is inserted, and so charged.
func TestTombstoneBudgetChargesOnlyInsertedTombstones(t *testing.T) {
	local, _ := newTombstoneBudgetMemory(1)
	scope := testScope("u1", "a")
	key := Key{Scope: scope, MsgID: 1}
	testAnswer(t, local, key, "ok")

	require.NoError(t, local.Cancel(key, testCancelled))
	require.NoError(t, local.CheckTombstone(scope), "a cancel of a live record charged nothing")

	require.NoError(t, local.Tombstone(key, testCancelled))
	require.Equal(t, RefusalTombstones, testRefused(t, local.CheckTombstone(scope)))
	checkInvariants(t, local)
}
