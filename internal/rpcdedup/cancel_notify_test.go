package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
