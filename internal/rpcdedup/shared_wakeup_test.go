package rpcdedup

import (
	"context"
	"errors"
	"fmt"
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

// watcherRunning reports whether the wake watcher of the fixture's node runs, or is starting.
func watcherRunning(fx *sharedFixture) bool {
	fx.shared.watchMu.Lock()
	defer fx.shared.watchMu.Unlock()
	return fx.shared.watching
}

// listWaiter lists nodeID as a waiter of the in-flight record of key, as its begin does.
func listWaiter(t *testing.T, fx *sharedFixture, key Key, fingerprint [32]byte, nodeID string) {
	t.Helper()
	_, err := fx.backend.begin(context.Background(), key, fingerprint, nodeID, time.Second, 64)
	require.NoError(t, err)
}

// fillWaiters lists count other nodes as waiters of the in-flight record of key, as their begins would.
func fillWaiters(t *testing.T, fx *sharedFixture, key Key, fingerprint [32]byte, count int) {
	t.Helper()
	for i := range count {
		listWaiter(t, fx, key, fingerprint, fmt.Sprintf("waiter-%d", i))
	}
}

func TestSharedWakeupRegistersWaiterOnRemoteInFlight(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, []string{"node-a"}, entry.waiters)

	// The same node begins again: it is listed once.
	_, err = fx.backend.begin(context.Background(), sharedKey, sharedFingerprintA, "node-a", time.Second, 64)
	require.NoError(t, err)
	entry, _ = fx.backend.entry(sharedKey)
	require.Equal(t, []string{"node-a"}, entry.waiters)

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
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
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
}

func TestSharedWakeupCompletesWithoutReading(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
	waitDone(t, owner)
	require.Equal(t, sharedWakeupRecord.Reply, owner.Record().Reply)
	require.Equal(t, 1, fx.pollCount())
	require.Equal(t, 1, fx.endCount(WatchEndWoken))
}

func TestSharedWakeupLostWakeIsAnsweredAtCheck(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.shared.checkDelay = 20 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The wake is lost: the check finds the answer.
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndChecked))
	require.Equal(t, 1, fx.pollCount())
}

func TestSharedWakeupCheckIsJittered(t *testing.T) {
	const samples = 1000
	distinct := make(map[time.Duration]struct{})
	for range samples {
		delay := newRetryBackOff(time.Second, time.Second).NextBackOff()
		require.GreaterOrEqual(t, delay, 800*time.Millisecond)
		require.LessOrEqual(t, delay, 1200*time.Millisecond)
		distinct[delay] = struct{}{}
	}
	require.Greater(t, len(distinct), 1, "the checks of a wave are not all at one time")
}

func TestWakeCheckDelayBelowInflightGrace(t *testing.T) {
	// The check, at most a fifth later than wakeCheckDelay, comes well before the deadline of the wait.
	require.Less(t, wakeCheckDelay*6/5, InflightGrace)
}

func TestSharedWakeupBeforeRemoteMarkIsKept(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	// The owner completes and wakes between the answer of the begin script and the marking of the flight.
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
	require.Equal(t, 1, fx.endCount(WatchEndWoken))
}

func TestSharedWakeupManyWakesCoalesce(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	for range 5 {
		fx.shared.Wake(sharedKey)
	}
	require.Eventually(t, func() bool {
		return fx.pollCount() >= 1
	}, 5*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.LessOrEqual(t, fx.pollCount(), 2)
	require.True(t, flightOpen(owner))

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
}

func TestSharedWakeupStaleWakeKeepsWaiting(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	fx.shared.Wake(sharedKey)
	require.Eventually(t, func() bool {
		return fx.pollCount() == 1
	}, 5*time.Second, time.Millisecond)
	require.True(t, flightOpen(owner), "the record is still in flight, so the wait goes on")
	require.Equal(t, 0, fx.endCount(WatchEndWoken))

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndWoken))
}

func TestSharedWakeupOwnerCrashReadsOnceThenDeadline(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.shared.checkDelay = 20 * time.Millisecond
	fx.shared.inflightGrace = 50 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, 10*time.Millisecond)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The owner died: the record stays in flight, and the deadline abandons the wait without a read.
	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
	require.Equal(t, 1, fx.endCount(WatchEndDeadline))
	require.Equal(t, 1, fx.pollCount())
}

func TestSharedWakeupRecordMissingAfterWake(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	fx.backend.remove(sharedKey)
	fx.shared.Wake(sharedKey)
	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
	require.Equal(t, 1, fx.endCount(WatchEndWoken))
}

func TestSharedWakeupFullRefusesWait(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	fillWaiters(t, fx, sharedKey, sharedFingerprintA, 64)

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	require.False(t, flightOpen(owner), "a refused duplicate is answered at once")
	require.True(t, owner.Record().Abandoned)
	require.True(t, owner.Record().NotRun)
	require.Equal(t, 1, fx.endCount(WatchEndRefused))
	require.Equal(t, 0, fx.pollCount())
	require.Equal(t, 0, fx.backend.watchStartCount())
	entry, _ := fx.backend.entry(sharedKey)
	require.Len(t, entry.waiters, 64)
	require.NotContains(t, entry.waiters, "node-a")
}

func TestSharedWakeupFullForwardsEarlierCancel(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	fillWaiters(t, fx, sharedKey, sharedFingerprintA, 64)
	// The duplicate is cancelled while its flight has no call to cancel: the cancel is forwarded to the owner.
	fx.backend.onBegin = func() {
		require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	}

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	requireNotifiedEventually(t, fx, []notifyCall{{nodeID: "node-b", key: sharedKey}})
	require.Equal(t, 1, fx.endCount(WatchEndRefused))
}

func TestSharedWakeupWatcherStartsOnFirstRemoteWait(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	otherKey := Key{Scope: sharedKey.Scope, MsgID: 8}
	fx.backend.put(otherKey, 'i', sharedFingerprintA, "node-b", Record{})
	require.Equal(t, 0, fx.backend.watchStartCount(), "no watcher before a remote wait")

	first := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, first)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return fx.backend.watchStartCount() == 1
	}, 5*time.Second, time.Millisecond)

	second := NewCallFlight(sharedFingerprintA, time.Second)
	_, err = fx.shared.Begin(otherKey, second)
	require.NoError(t, err)
	require.Equal(t, 1, fx.backend.watchStartCount(), "a running watcher is not started again")

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.finish(otherKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
	fx.backend.deliverWake(otherKey)
	waitDone(t, first)
	waitDone(t, second)
}

func TestSharedWakeupWatcherStopsWhenIdle(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.shared.watchIdleAfter = 200 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
	waitDone(t, owner)

	// Inside the idle time the empty read keeps the watcher.
	fx.backend.emptyRead()
	require.True(t, watcherRunning(fx))

	// Once the idle time has passed, an empty read stops it.
	require.Eventually(t, func() bool {
		fx.backend.emptyRead()
		return !watcherRunning(fx)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, fx.backend.watchStartCount())
}

func TestSharedWakeupWatcherRestartsAfterIdle(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.shared.watchIdleAfter = 20 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	first := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, first)
	require.NoError(t, err)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
	waitDone(t, first)
	require.Eventually(t, func() bool {
		fx.backend.emptyRead()
		return !watcherRunning(fx)
	}, 5*time.Second, 10*time.Millisecond)

	otherKey := Key{Scope: sharedKey.Scope, MsgID: 8}
	fx.backend.put(otherKey, 'i', sharedFingerprintA, "node-b", Record{})
	second := NewCallFlight(sharedFingerprintA, time.Second)
	_, err = fx.shared.Begin(otherKey, second)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return fx.backend.watchStartCount() == 2
	}, 5*time.Second, time.Millisecond)

	fx.backend.finish(otherKey, sharedWakeupRecord)
	fx.backend.deliverWake(otherKey)
	waitDone(t, second)
	require.Equal(t, 2, fx.endCount(WatchEndWoken), "both waits were woken, one before the idle stop and one after the restart")
}

func TestSharedWakeupWatcherNotStoppedWhileWaiting(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.shared.watchIdleAfter = time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)

	// The wait is open, so the watcher is not idle, however long ago the last wait ended.
	time.Sleep(5 * time.Millisecond)
	for range 3 {
		fx.backend.emptyRead()
	}
	require.True(t, watcherRunning(fx))

	fx.backend.finish(sharedKey, sharedWakeupRecord)
	fx.backend.deliverWake(sharedKey)
	waitDone(t, owner)
}

func TestSharedCompleteWakesWaiters(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-b")
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-c")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.False(t, flightOpen(owner))
	require.Equal(t, []wakeCall{{key: sharedKey, nodes: []string{"node-b", "node-c"}}}, fx.backend.wakeCalls())
	require.Equal(t, [2]int{2, 0}, fx.woke)
}

func TestSharedCompleteWakeFailureIsCounted(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-b")
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-c")
	fx.backend.wakeErr = errors.New("wake failed")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Equal(t, [2]int{0, 2}, fx.woke)
	require.False(t, flightOpen(owner))
	require.Equal(t, sharedWakeupRecord.Reply, owner.Record().Reply, "the local record is kept")
}

func TestSharedCompleteAbandonedWakesWaiters(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-b")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Abandoned: true, NotRun: true}))
	require.Equal(t, []wakeCall{{key: sharedKey, nodes: []string{"node-b"}}}, fx.backend.wakeCalls())
}

func TestSharedCompleteNotOwnedWakesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-c")
	// The flight was never begun here: the record belongs to node-b, so this write does not take effect.
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.backend.wakeCalls())
}

func TestSharedCompleteLocalOnlyWakesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	owner.localOnly = true
	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.backend.wakeCalls())
	require.Empty(t, fx.backend.callLog())
}

func TestSharedCompleteFailedWriteWakesNobody(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-b")
	fx.backend.setFail(errors.New("redis is down"))

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.backend.wakeCalls())
}

func TestSharedCompleteWithoutWakeupWakesNobody(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	listWaiter(t, fx, sharedKey, sharedFingerprintA, "node-b")

	require.NoError(t, fx.shared.Complete(sharedKey, owner, sharedWakeupRecord))
	require.Empty(t, fx.backend.wakeCalls())
}

func TestSharedWithoutWakeupPollsAsBefore(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)
	require.Equal(t, 1, fx.endCount(WatchEndPolled))
	require.Equal(t, 0, fx.endCount(WatchEndWoken))
	require.Equal(t, 0, fx.endCount(WatchEndChecked))
}

func TestSharedRunWithoutWakeupStartsNoWatcher(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fx.shared.Run(ctx)
	}()

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.finish(sharedKey, sharedWakeupRecord)
	waitDone(t, owner)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, 0, fx.backend.watchStartCount())
}

func TestSharedRunStopsWatcher(t *testing.T) {
	fx := newSharedFixtureWith(t, false, true)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fx.shared.Run(ctx)
	}()

	owner := NewCallFlight(sharedFingerprintA, 5*time.Millisecond)
	fx.shared.inflightGrace = 5 * time.Millisecond
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return watcherRunning(fx)
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, watcherRunning(fx), "Run returns after the watcher returned")
	waitDone(t, owner)
}
