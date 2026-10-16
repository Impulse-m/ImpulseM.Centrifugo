package rpcdedup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sharedWakeupRecord is the answer of the calls of the wakeup tests.
var sharedWakeupRecord = Record{Reply: []byte("answer")}

// flightOpen reports whether flight is still running.
func flightOpen(flight *Flight) bool {
	select {
	case <-flight.Done():
		return false
	default:
		return true
	}
}

func TestSharedWakeupRegistersWaiterOnRemoteInFlight(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, []string{"node-a"}, entry.waiters)

	// The same node begins again: it is listed once.
	_, err = fx.backend.Begin(context.Background(), sharedKey, sharedFingerprintA, "node-a", time.Second, true)
	require.NoError(t, err)
	entry, _ = fx.backend.entry(sharedKey)
	require.Equal(t, []string{"node-a"}, entry.waiters)

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
}

func TestSharedWithoutWakeupDoesNotRegister(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Empty(t, entry.waiters)

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Empty(t, fx.doneNotified)
}

func TestSharedWakeupCompletesWithoutPolling(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
	require.Equal(t, sharedWakeupRecord.Reply, owner.Record().Reply)
	require.Equal(t, 1, fx.pollCount())
	require.Equal(t, 1, fx.endCount(WatchEndNotified))
}

func TestSharedWakeupLostNotificationFallsBackToPoll(t *testing.T) {
	fx := newSharedFixtureWakeup(t, 20*time.Millisecond)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The notification is lost: the answer is found by the fallback poll.
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
	require.GreaterOrEqual(t, fx.pollCount(), 1)
	require.LessOrEqual(t, fx.pollCount(), 5)
}

func TestSharedWakeupBeforeRemoteMarkIsKept(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	// The owner completes and notifies between the answer of the begin script and the marking of the flight.
	fx.backend.afterBegin = func() {
		fx.backend.finish(sharedKey, sharedWakeupRecord)
		fx.shared.Wake(sharedKey)
	}

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waitDone(t, owner)
	require.Equal(t, sharedWakeupRecord.Reply, owner.Record().Reply)
	require.Equal(t, 1, fx.pollCount())
	require.Equal(t, 1, fx.endCount(WatchEndNotified))
}

func TestSharedWakeupManyNotificationsDoNotBlock(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The record is still in flight: the notifications coalesce, and the call keeps waiting.
	for range 100 {
		fx.shared.Wake(sharedKey)
	}
	require.Eventually(t, func() bool {
		return fx.pollCount() >= 1
	}, 5*time.Second, time.Millisecond)
	require.True(t, flightOpen(owner))

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndNotified))
}

func TestSharedWakeupStaleNotificationKeepsWaiting(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// A notification of an earlier record: the poll finds the record in flight, and the wait goes on.
	fx.shared.Wake(sharedKey)
	require.Eventually(t, func() bool {
		return fx.pollCount() == 1
	}, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return !flightOpen(owner)
	}, 50*time.Millisecond, time.Millisecond)
	require.Equal(t, 0, fx.endCount(WatchEndNotified))

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndNotified))
}

func TestSharedWakeupOwnerCrashEndsAtExpiry(t *testing.T) {
	fx := newSharedFixtureWakeup(t, 20*time.Millisecond)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The owner died: its record expires, and no notification comes.
	fx.backend.remove(sharedKey)
	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
	require.False(t, owner.Record().NotRun)
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
}

func TestSharedWakeupDeadlineWithoutAnswer(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	fx.shared.inflightGrace = 20 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, 0)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
	require.Equal(t, 1, fx.endCount(WatchEndDeadline))
	require.Equal(t, 0, fx.pollCount())
}

func TestSharedWakeUnknownKeyIsIgnored(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	require.NotPanics(t, func() {
		fx.shared.Wake(Key{Scope: testScope("u9", "b"), MsgID: 99})
	})
}

func TestSharedWakeOwnerFlightIsIgnored(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)

	fx.shared.Wake(sharedKey)
	require.True(t, flightOpen(owner), "a notification does not complete a call which runs on this node")
	require.Empty(t, fx.doneNotified)

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	waitDone(t, owner)
}

// ownerFlight returns a flight which runs on node-a: its begin registered the call in the fake backend.
func ownerFlight(t *testing.T, fx *sharedFixture) *Flight {
	t.Helper()
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	return owner
}

// setWaiters sets the waiters of the in-flight record of sharedKey in the fake backend.
func setWaiters(fx *sharedFixture, waiters ...string) {
	fx.backend.mu.Lock()
	defer fx.backend.mu.Unlock()
	fx.backend.entries[sharedKey].waiters = waiters
}

func TestSharedCompleteNotifiesWaiters(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b", "node-c", "node-a")

	// The notifier runs after the local flight is done, so the duplicates are answered before any waiter is told.
	var doneAtNotify []bool
	fx.shared.doneNotifier = func(nodeID string, key Key) {
		doneAtNotify = append(doneAtNotify, !flightOpen(owner))
		fx.doneNotified = append(fx.doneNotified, notifyCall{nodeID: nodeID, key: key})
	}
	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Equal(t, []notifyCall{{nodeID: "node-b", key: sharedKey}, {nodeID: "node-c", key: sharedKey}}, fx.doneNotified)
	require.Equal(t, []bool{true, true}, doneAtNotify)
}

func TestSharedCompleteAbandonedNotifiesWaiters(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Abandoned: true}))
	require.Equal(t, []notifyCall{{nodeID: "node-b", key: sharedKey}}, fx.doneNotified)
	_, found := fx.backend.entry(sharedKey)
	require.False(t, found)
}

func TestSharedCompleteNotOwnedNotifiesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")
	fx.backend.mu.Lock()
	fx.backend.entries[sharedKey].owner = "node-z"
	fx.backend.mu.Unlock()

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.doneNotified)
}

func TestSharedCompleteLocalOnlyNotifiesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")
	owner.localOnly = true

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.doneNotified)
}

func TestSharedCompleteFailedWriteNotifiesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")
	fx.backend.mu.Lock()
	fx.backend.completeFailures = sharedCompleteAttempts
	fx.backend.mu.Unlock()

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.doneNotified)
}

func TestSharedCompleteWithoutWakeupNotifiesNobody(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.doneNotified)
}

func TestSharedWithoutWakeupPollsAsBefore(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
	require.Equal(t, 0, fx.endCount(WatchEndNotified))
}

// The owner's reply waits for the done notifier when a remote waiter exists: Complete returns once the local flight is
// done and the notifier ran.
func TestSharedCompleteReturnsAfterSlowNotifier(t *testing.T) {
	fx := newSharedFixtureWakeup(t, time.Hour)
	owner := ownerFlight(t, fx)
	setWaiters(fx, "node-b")
	var notifiedAt time.Time
	fx.shared.doneNotifier = func(string, Key) {
		time.Sleep(5 * time.Millisecond)
		notifiedAt = time.Now()
	}

	start := time.Now()
	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	elapsed := time.Since(start)
	require.False(t, notifiedAt.IsZero(), "the notifier ran before Complete returned")
	require.False(t, flightOpen(owner))
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
	t.Logf("owner reply with one remote waiter and a notifier of 5ms: %s", elapsed)
}

func TestSharedWakeupEndsWithErrorKept(t *testing.T) {
	fx := newSharedFixtureWakeup(t, 20*time.Millisecond)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// Failing polls are retried at the fallback interval, and the wait goes on: the answer is found once the poll works.
	fx.backend.setFail(errors.New("redis is down"))
	require.Eventually(t, func() bool {
		return fx.pollCount() >= 2
	}, 5*time.Second, time.Millisecond)
	require.True(t, flightOpen(owner))
	fx.backend.setFail(nil)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Equal(t, sharedWakeupRecord.Reply, owner.Record().Reply)
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
}
