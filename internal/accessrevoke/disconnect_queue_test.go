package accessrevoke

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// fakeConnection models a Core client for the queue: BeginDisconnect marks it closing and records the disconnect once,
// and Closing reports the mark. It stays in the session index, so it models a close which has not completed.
type fakeConnection struct {
	id          string
	closing     atomic.Bool
	mu          sync.Mutex
	disconnects []centrifuge.Disconnect
	times       []time.Time
}

// ID implements Connection.
func (c *fakeConnection) ID() string {
	return c.id
}

// BeginDisconnect implements Connection. Like Client.BeginDisconnect, a call on a closing connection does nothing.
func (c *fakeConnection) BeginDisconnect(disconnect centrifuge.Disconnect) bool {
	if c.closing.Swap(true) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnects = append(c.disconnects, disconnect)
	c.times = append(c.times, time.Now())
	return true
}

// Closing implements Connection.
func (c *fakeConnection) Closing() bool {
	return c.closing.Load()
}

// markClosing stands for a connection which closes by itself, not through the queue.
func (c *fakeConnection) markClosing() {
	c.closing.Store(true)
}

func (c *fakeConnection) snapshot() ([]centrifuge.Disconnect, []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]centrifuge.Disconnect(nil), c.disconnects...), append([]time.Time(nil), c.times...)
}

// queuedCount returns the number of connections the queue holds as queued.
func queuedCount(queue *DisconnectQueue) int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.inflight)
}

func runQueue(t *testing.T, queue *DisconnectQueue) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- queue.Run(ctx)
	}()
	return cancel, errCh
}

func TestDisconnectQueuePacesDisconnects(t *testing.T) {
	// 20 per second means one disconnect every 50ms.
	queue := NewDisconnectQueue(20, 100)
	conns := make([]*fakeConnection, 0, 5)
	for i := range 5 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		conns = append(conns, conn)
		require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	}

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		for _, conn := range conns {
			disconnects, _ := conn.snapshot()
			if len(disconnects) != 1 {
				return false
			}
		}
		return true
	}, 5*time.Second, 5*time.Millisecond)

	// The queue is FIFO, so the connections are disconnected in the order they were enqueued.
	times := make([]time.Time, 0, len(conns))
	for _, conn := range conns {
		disconnects, connTimes := conn.snapshot()
		require.Equal(t, centrifuge.DisconnectExpired, disconnects[0])
		times = append(times, connTimes[0])
	}
	// Four gaps of 50ms between five disconnects; allow some slack for timers.
	require.GreaterOrEqual(t, times[4].Sub(times[0]), 150*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestDisconnectQueueDropsWhenFull(t *testing.T) {
	// Queue is not drained, so the third disconnect does not fit.
	queue := NewDisconnectQueue(1000, 2)
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	third := &fakeConnection{id: "third"}
	dropped := counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total")

	require.True(t, queue.Enqueue(first, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.Enqueue(second, centrifuge.DisconnectExpired, ReasonSession))
	require.False(t, queue.Enqueue(third, centrifuge.DisconnectExpired, ReasonSession))

	require.Equal(t, dropped+1, counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total"))
	disconnects, _ := third.snapshot()
	require.Empty(t, disconnects)
}

// TestDisconnectQueueEnqueuesConnectionOnce enqueues the same connection twice while it is queued. The queue holds one
// disconnect for it. After the disconnect the connection is closing, so a later enqueue is handled without an item and
// without a second disconnect.
func TestDisconnectQueueEnqueuesConnectionOnce(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	conn := &fakeConnection{id: "once"}

	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.Len(t, queue.items, 1)

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		disconnects, _ := conn.snapshot()
		return len(disconnects) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return queuedCount(queue) == 0
	}, 5*time.Second, 5*time.Millisecond)
	require.True(t, conn.Closing())
	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.Len(t, queue.items, 0)
	require.Never(t, func() bool {
		disconnects, _ := conn.snapshot()
		return len(disconnects) > 1
	}, 100*time.Millisecond, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// TestDisconnectQueueTerminalReplacesReconnectable queues a connection with a reconnectable disconnect, then with a terminal
// one, then with a reconnectable one again. The connection is disconnected once, with the terminal code, so a block which
// arrives while the queue holds an expiry is not lost.
func TestDisconnectQueueTerminalReplacesReconnectable(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	conn := &fakeConnection{id: "upgrade"}

	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.Enqueue(conn, DisconnectUserBlocked, ReasonBlocked))
	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.Len(t, queue.items, 1)

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		disconnects, _ := conn.snapshot()
		return len(disconnects) == 1
	}, 5*time.Second, 5*time.Millisecond)
	disconnects, _ := conn.snapshot()
	require.Equal(t, DisconnectUserBlocked, disconnects[0])

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// TestDisconnectQueueSkipsClosingConnection enqueues a connection whose close has begun into a full queue. The enqueue
// counts as handled: nothing is queued, nothing is kept, and the drop counter does not move.
func TestDisconnectQueueSkipsClosingConnection(t *testing.T) {
	queue := NewDisconnectQueue(1000, 1)
	filler := &fakeConnection{id: "filler"}
	require.True(t, queue.Enqueue(filler, centrifuge.DisconnectExpired, ReasonSession))
	closing := &fakeConnection{id: "closing"}
	closing.markClosing()
	dropped := counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total")

	require.True(t, queue.enqueueKeyed(retryKey{user: "user-1"}, closing, centrifuge.DisconnectExpired, ReasonBlocked))

	require.Len(t, queue.items, 1)
	require.Equal(t, 1, queuedCount(queue))
	queue.mu.Lock()
	require.Empty(t, queue.retry)
	queue.mu.Unlock()
	require.Equal(t, dropped, counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total"))
}

// TestDisconnectQueueSkipsConnectionClosedWhileQueued queues two connections at 1 per second, then closes the first before
// the queue runs. The second is disconnected at once, so the first spent no rate token, and only the second is counted.
func TestDisconnectQueueSkipsConnectionClosedWhileQueued(t *testing.T) {
	queue := NewDisconnectQueue(1, 10)
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	require.True(t, queue.Enqueue(first, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.Enqueue(second, centrifuge.DisconnectExpired, ReasonSession))
	first.markClosing()
	counted := labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonSession))

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		disconnects, _ := second.snapshot()
		return len(disconnects) == 1
	}, 500*time.Millisecond, 5*time.Millisecond)
	firstDisconnects, _ := first.snapshot()
	require.Empty(t, firstDisconnects)
	require.Equal(t, counted+1, labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonSession)))

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// retryRecorder returns a retrier which records the length of the queue at each call and queues nothing.
func retryRecorder(queue *DisconnectQueue) (func(key retryKey), *[]int, *sync.Mutex) {
	var mu sync.Mutex
	lengths := []int{}
	retrier := func(key retryKey) {
		mu.Lock()
		defer mu.Unlock()
		lengths = append(lengths, len(queue.items))
	}
	return retrier, &lengths, &mu
}

// TestDisconnectQueueRetriesOneKeyWhileItemsQueued fills a queue of 4 and drops three keys. The retries run one per
// dequeue while the queue holds items, so the queue lengths seen by the retrier are 2, 1 and 0.
func TestDisconnectQueueRetriesOneKeyWhileItemsQueued(t *testing.T) {
	queue := NewDisconnectQueue(1000, 4)
	retrier, lengths, mu := retryRecorder(queue)
	queue.setRetrier(retrier)
	for i := range 4 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	for i := range 3 {
		conn := &fakeConnection{id: "dropped-" + strconv.Itoa(i)}
		key := retryKey{user: "user-" + strconv.Itoa(i)}
		require.False(t, queue.enqueueKeyed(key, conn, centrifuge.DisconnectExpired, ReasonBlocked))
	}

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(*lengths) == 3
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(*lengths) > 3
	}, 100*time.Millisecond, 5*time.Millisecond)
	mu.Lock()
	require.Equal(t, []int{2, 1, 0}, *lengths)
	mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// TestDisconnectQueueRetriesNextKeyWhileEmpty fills a queue of 2 and drops three keys. The first retry runs on the
// dequeue which leaves one item. The queue is then empty, so the next keys run in a row, each once: the lengths are 1, 0, 0.
func TestDisconnectQueueRetriesNextKeyWhileEmpty(t *testing.T) {
	queue := NewDisconnectQueue(1000, 2)
	retrier, lengths, mu := retryRecorder(queue)
	queue.setRetrier(retrier)
	for i := range 2 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	for i := range 3 {
		conn := &fakeConnection{id: "dropped-" + strconv.Itoa(i)}
		key := retryKey{user: "user-" + strconv.Itoa(i)}
		require.False(t, queue.enqueueKeyed(key, conn, centrifuge.DisconnectExpired, ReasonBlocked))
	}

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(*lengths) == 3
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(*lengths) > 3
	}, 100*time.Millisecond, 5*time.Millisecond)
	mu.Lock()
	require.Equal(t, []int{1, 0, 0}, *lengths)
	mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// TestDisconnectQueueRetriesDroppedKeyWhenHalfFree drops a keyed disconnect from a full queue. The key is re-evaluated
// once the queue is at most half full, and only then, and it is removed by the retry, so it is evaluated once.
func TestDisconnectQueueRetriesDroppedKeyWhenHalfFree(t *testing.T) {
	queue := NewDisconnectQueue(1000, 4)
	var mu sync.Mutex
	var retried []int
	queue.setRetrier(func(key retryKey) {
		mu.Lock()
		defer mu.Unlock()
		retried = append(retried, len(queue.items))
	})
	key := retryKey{user: "user-1"}
	for i := range 4 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(key, conn, centrifuge.DisconnectExpired, ReasonBlocked))
	}
	dropped := &fakeConnection{id: "dropped"}
	require.False(t, queue.enqueueKeyed(key, dropped, centrifuge.DisconnectExpired, ReasonBlocked))

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(retried) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(retried) > 1
	}, 100*time.Millisecond, 5*time.Millisecond)
	mu.Lock()
	require.LessOrEqual(t, retried[0], 2)
	mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestDisconnectQueueRunReturnsOnContextDone(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	cancel, errCh := runQueue(t, queue)

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("queue Run did not return after context cancel")
	}
}
