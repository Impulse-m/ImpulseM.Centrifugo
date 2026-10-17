package rpcdedup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runAckFlush runs the ack flush of fx until the test ends, as Run does in production.
func runAckFlush(t *testing.T, fx *sharedFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fx.shared.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// flushedAcks counts the keys of the ack batches which were flushed.
func flushedAcks(batches [][]ScopeAcks) int {
	var keys int
	for _, batch := range batches {
		for _, acks := range batch {
			keys += len(acks.MsgIDs)
		}
	}
	return keys
}

// With no acks queued, the flush makes no backend call, however long the node runs.
func TestSharedAckFlushMakesNoBackendCallWithoutAcks(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackFlushEvery = 5 * time.Millisecond
	runAckFlush(t, fx)

	time.Sleep(100 * time.Millisecond)
	require.Empty(t, fx.backend.ackBatches())
	require.Equal(t, 0, countCalls(fx.backend.callLog(), "ack"))
}

// An ack which is alone in its batch is flushed once the flush interval has passed.
func TestSharedAckFlushedWithinInterval(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackFlushEvery = 20 * time.Millisecond
	scope := testScope("u1", "a")
	testAnswer(t, fx.local, Key{Scope: scope, MsgID: 1}, "ok")
	require.NoError(t, fx.shared.Ack(scope, []uint64{1}))
	runAckFlush(t, fx)

	require.Eventually(t, func() bool {
		return flushedAcks(fx.backend.ackBatches()) == 1
	}, 5*time.Second, time.Millisecond)
	require.Len(t, fx.backend.ackBatches(), 1)
}

// A batch which reaches its size limit is flushed at once, even when the flush interval is far away, and a batch
// below the limit waits for the interval.
func TestSharedAckFullBatchFlushesAtOnce(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackFlushEvery = time.Hour
	fx.shared.ackBatch = 3
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 3; msgID++ {
		testAnswer(t, fx.local, Key{Scope: scope, MsgID: msgID}, "ok")
	}
	require.NoError(t, fx.shared.Ack(scope, []uint64{1, 2}))
	runAckFlush(t, fx)

	require.Never(t, func() bool {
		return len(fx.backend.ackBatches()) > 0
	}, 50*time.Millisecond, time.Millisecond, "a batch below its size limit waits for the interval")

	require.NoError(t, fx.shared.Ack(scope, []uint64{3}))
	require.Eventually(t, func() bool {
		return flushedAcks(fx.backend.ackBatches()) == 3
	}, 5*time.Second, time.Millisecond, "a full batch is flushed without waiting for the interval")
	require.Len(t, fx.backend.ackBatches(), 1)
}

// After a full batch flushes and leaves the batch empty, a later ack starts a new interval and is flushed by it.
func TestSharedAckFlushRearmsAfterFullBatch(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackFlushEvery = 20 * time.Millisecond
	fx.shared.ackBatch = 2
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 3; msgID++ {
		testAnswer(t, fx.local, Key{Scope: scope, MsgID: msgID}, "ok")
	}
	runAckFlush(t, fx)

	require.NoError(t, fx.shared.Ack(scope, []uint64{1, 2}))
	require.Eventually(t, func() bool {
		return flushedAcks(fx.backend.ackBatches()) == 2
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, fx.shared.Ack(scope, []uint64{3}))
	require.Eventually(t, func() bool {
		return flushedAcks(fx.backend.ackBatches()) == 3
	}, 5*time.Second, time.Millisecond)

	batches := fx.backend.ackBatches()
	require.Len(t, batches, 2)
	require.Equal(t, 1, flushedAcks(batches[1:]))
}
