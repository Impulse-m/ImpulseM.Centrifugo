package rpcdedup

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// With the wakeup off, the poll of a wait of a long call is spaced by a tenth of its length, not by PollMax alone, so a
// wait for a call which runs for half a minute reads Redis at most about ten times in ten seconds, not about twenty. The
// fake clock of the bubble makes the waiting free and exact.
func TestRemoteWaitPollBacksOffForLongDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := newSharedFixture(t, false)
		// The intervals of a production node: a ramp from 50 ms to PollMax of 500 ms.
		fx.shared.pollInitial = 50 * time.Millisecond
		fx.shared.pollMax = 500 * time.Millisecond
		fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

		owner := NewCallFlight(sharedFingerprintA, 30*time.Second)
		_, err := fx.shared.Begin(sharedKey, owner)
		require.NoError(t, err)

		time.Sleep(10 * time.Second)
		synctest.Wait()
		polls := countCalls(fx.backend.callLog(), "poll")

		// The answer arrives, so the wait ends and the bubble can end.
		fx.backend.finish(sharedKey, sharedWakeupRecord)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.LessOrEqual(t, polls, 12, "the polls of a long wait are spaced out")
	})
}

// The poll interval of a wait is PollMax at most for a short call, and a tenth of the call for a long one.
func TestSharedPollCapScalesWithWait(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.pollMax = 500 * time.Millisecond
	require.Equal(t, 500*time.Millisecond, fx.shared.pollCap(5*time.Second), "a short wait keeps PollMax")
	require.Equal(t, 3*time.Second, fx.shared.pollCap(30*time.Second))
	require.Equal(t, 500*time.Millisecond, fx.shared.pollCap(0), "no wait has a shorter cap than PollMax")
}
