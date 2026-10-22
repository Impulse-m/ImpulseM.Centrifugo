package rpcdedup

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingCancelNotifier replaces the notifier of fx with one which blocks until release is closed. It counts the
// notifications which run at once, and the most of them which ran at once.
type blockingCancelNotifier struct {
	running atomic.Int32
	peak    atomic.Int32
	release chan struct{}
}

// newBlockingCancelNotifier returns a notifier which blocks until its release is closed.
func newBlockingCancelNotifier() *blockingCancelNotifier {
	return &blockingCancelNotifier{release: make(chan struct{})}
}

// notify is the notifier of the store: it counts the notification while it blocks.
func (n *blockingCancelNotifier) notify(_ string, _ Key) {
	running := n.running.Add(1)
	for {
		peak := n.peak.Load()
		if running <= peak || n.peak.CompareAndSwap(peak, running) {
			break
		}
	}
	<-n.release
	n.running.Add(-1)
}

// A burst of cancels of one call sends one notification at a time, not one goroutine per cancel: the cancels which come
// while the notification of the call is pending are coalesced into it.
func TestSharedCancelNotifyCoalesced(t *testing.T) {
	notifier := newBlockingCancelNotifier()
	fx := newSharedFixtureWithNotifier(t, notifier.notify)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	for range 1000 {
		require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	}
	require.Eventually(t, func() bool { return notifier.running.Load() == 1 }, 2*time.Second, time.Millisecond)
	require.Never(t, func() bool { return notifier.running.Load() > 1 }, 100*time.Millisecond, time.Millisecond, "one notification of the call at a time")

	close(notifier.release)
	require.Eventually(t, func() bool { return notifier.running.Load() == 0 }, 2*time.Second, time.Millisecond)
	require.LessOrEqual(t, notifier.peak.Load(), int32(1))
}

// Cancels of many calls send their notifications through a bounded set of workers, not one goroutine per cancel.
func TestSharedCancelNotifyBoundedWorkers(t *testing.T) {
	notifier := newBlockingCancelNotifier()
	fx := newSharedFixtureWithNotifier(t, notifier.notify)

	const calls = 100
	for i := range calls {
		key := Key{Scope: sharedKey.Scope, MsgID: uint64(i + 1)}
		fx.backend.put(key, 'i', sharedFingerprintA, "node-b", Record{})
		require.NoError(t, fx.shared.Cancel(key, testCancelled))
	}
	require.Eventually(t, func() bool { return notifier.running.Load() == maxCancelWorkers }, 2*time.Second, time.Millisecond)
	require.Never(t, func() bool { return notifier.running.Load() > maxCancelWorkers }, 100*time.Millisecond, time.Millisecond)

	close(notifier.release)
	require.Eventually(t, func() bool { return notifier.running.Load() == 0 }, 2*time.Second, time.Millisecond)
	require.LessOrEqual(t, notifier.peak.Load(), int32(maxCancelWorkers))
}

// The notices which wait for a worker are capped: a notice beyond maxCancelPending is dropped, and notify reports it.
func TestCancelNotifierDropsBeyondCap(t *testing.T) {
	release := make(chan struct{})
	notifier := newCancelNotifier(func(string, Key) { <-release })
	defer close(release)

	const extra = 10
	const notices = maxCancelPending + maxCancelWorkers + extra
	accepted := 0
	for i := range notices {
		key := Key{Scope: sharedKey.Scope, MsgID: uint64(i + 1)}
		if notifier.notify("node-b", key) {
			accepted++
		}
	}
	// The workers take at most maxCancelWorkers notices off the queue, and they block until release.
	require.LessOrEqual(t, accepted, maxCancelPending+maxCancelWorkers)
	require.GreaterOrEqual(t, notices-accepted, extra)
}
