package rpcdedup

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requireWakeEventually waits until the wakes of a completed call are written and counted. The owner does not wait for
// them, see Shared.wakeWaiters, so a test reads them once they are written.
func requireWakeEventually(t *testing.T, fx *sharedFixture, calls []wakeCall, woke [2]int) {
	t.Helper()
	require.Eventually(t, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return fx.woke == woke && len(fx.backend.wakeCalls()) == len(calls)
	}, 2*time.Second, time.Millisecond, "the wakes of the call are written")
	require.Equal(t, calls, fx.backend.wakeCalls())
}

// The owner replies without waiting for the wakes of its waiters: a wake which the shared tier is slow to take does not
// hold the completion of the call.
func TestSharedCompleteRepliesBeforeWake(t *testing.T) {
	fx := newSharedFixtureWakeup(t)
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fillWaiters(t, fx, sharedKey, sharedFingerprintA, 1)

	release := make(chan struct{})
	var wakeReturned atomic.Bool
	fx.backend.onWake = func() {
		// The wake is held until the test releases it, or for a second at most, so a wake which holds Complete ends.
		select {
		case <-release:
		case <-time.After(time.Second):
		}
		wakeReturned.Store(true)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.False(t, wakeReturned.Load(), "Complete returns while the wake is held")
	require.Equal(t, []byte("ok"), owner.Record().Reply)

	close(release)
	require.Eventually(t, wakeReturned.Load, 2*time.Second, time.Millisecond)
	require.Len(t, fx.backend.wakeCalls(), 1)
}
