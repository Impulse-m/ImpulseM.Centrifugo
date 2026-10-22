package accessrevoke

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/stretchr/testify/require"
)

// testRetryKey is the key of the disconnects which the queue tests enqueue.
var testRetryKey = retryKey{user: "test-user"}

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
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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

	require.True(t, queue.enqueueKeyed(testRetryKey, first, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.enqueueKeyed(testRetryKey, second, centrifuge.DisconnectExpired, ReasonSession))
	require.False(t, queue.enqueueKeyed(testRetryKey, third, centrifuge.DisconnectExpired, ReasonSession))

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

	require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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
	require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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

	require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.enqueueKeyed(testRetryKey, conn, DisconnectUserBlocked, ReasonBlocked))
	require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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
	require.True(t, queue.enqueueKeyed(testRetryKey, filler, centrifuge.DisconnectExpired, ReasonSession))
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
	require.True(t, queue.enqueueKeyed(testRetryKey, first, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.enqueueKeyed(testRetryKey, second, centrifuge.DisconnectExpired, ReasonSession))
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

// TestDisconnectQueueRetriesOneKeyWhileItemsQueued fills a queue of 4 and drops three keys. The retries run once the queue
// is at most half full, one per dequeue while the queue holds items, so the queue lengths seen by the retrier are 2, 1 and 0.
func TestDisconnectQueueRetriesOneKeyWhileItemsQueued(t *testing.T) {
	queue := NewDisconnectQueue(1000, 4)
	retrier, lengths, mu := retryRecorder(queue)
	queue.setRetrier(retrier)
	for i := range 4 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
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

// TestDisconnectQueueRetriesDroppedKeyWhenRoom drops a keyed disconnect from a full queue of 4. The key is re-evaluated
// once the queue is at most half full, at the second dequeue, and it is removed by the retry, so it is evaluated once.
func TestDisconnectQueueRetriesDroppedKeyWhenRoom(t *testing.T) {
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
	require.Equal(t, 2, retried[0])
	mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// TestDisconnectQueueRetriesOnceAtHalf drops a keyed disconnect from a full queue of 10 and dequeues four items. The key is
// not retried while the queue is above half, so a burst does not run a pass per dequeue. It is retried at the dequeue
// which leaves 5 items, the first point at which the queue is at most half full.
func TestDisconnectQueueRetriesOnceAtHalf(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	retrier, lengths, _ := retryRecorder(queue)
	queue.setRetrier(retrier)
	for i := range 10 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	dropped := &fakeConnection{id: "dropped"}
	require.False(t, queue.enqueueKeyed(retryKey{user: "user-1"}, dropped, centrifuge.DisconnectExpired, ReasonBlocked))

	for range 4 {
		select {
		case conn := <-queue.items:
			queue.begin(conn)
		default:
			t.Fatal("queue ran empty")
		}
	}
	require.Empty(t, *lengths)

	select {
	case conn := <-queue.items:
		queue.begin(conn)
	default:
		t.Fatal("queue ran empty")
	}
	require.Equal(t, []int{5}, *lengths)
}

// TestDisconnectQueueRetryGateIsHalfOfOddSize drops a keyed disconnect from a full queue of 9. The gate is cap/2 with integer
// division, which is 4. The key is not retried at 5 items, and it is retried at the dequeue which leaves 4.
func TestDisconnectQueueRetryGateIsHalfOfOddSize(t *testing.T) {
	queue := NewDisconnectQueue(1000, 9)
	retrier, lengths, _ := retryRecorder(queue)
	queue.setRetrier(retrier)
	for i := range 9 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	dropped := &fakeConnection{id: "dropped"}
	require.False(t, queue.enqueueKeyed(retryKey{user: "user-1"}, dropped, centrifuge.DisconnectExpired, ReasonBlocked))

	for range 4 {
		select {
		case conn := <-queue.items:
			queue.begin(conn)
		default:
			t.Fatal("queue ran empty")
		}
	}
	require.Empty(t, *lengths)

	select {
	case conn := <-queue.items:
		queue.begin(conn)
	default:
		t.Fatal("queue ran empty")
	}
	require.Equal(t, []int{4}, *lengths)
}

// TestDisconnectQueueFillGauge queues three connections and reads the fill gauge, which reads the queue at each scrape.
func TestDisconnectQueueFillGauge(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	metrics.BindAccessRevokeDisconnectQueue(queue.fill)
	defer metrics.BindAccessRevokeDisconnectQueue(nil)
	for i := range 3 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	}

	require.Equal(t, 3.0, counterValue(t, "centrifugo_access_revoke_disconnect_queue_fill"))
}

// TestDisconnectQueueHoldsKeyDroppedByRetry gives one user 100 connections and a queue of 10, and dequeues 60 times. Each
// retry pass of the user re-drops most of its connections, so the key is held until the queue is at or below half, and a
// pass runs about once per 5 dequeues. A pass per dequeue would scan the user 60 times, and the bound is 13 passes.
// The drops of the passes are not counted as dropped disconnects, so only the first drop counts.
func TestDisconnectQueueHoldsKeyDroppedByRetry(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	for i := range 10 {
		conn := &fakeConnection{id: "fill-" + strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	userConns := make([]*fakeConnection, 100)
	for i := range userConns {
		userConns[i] = &fakeConnection{id: "user-" + strconv.Itoa(i)}
	}
	userKey := retryKey{user: "heavy-user"}
	require.False(t, queue.enqueueKeyed(userKey, userConns[0], centrifuge.DisconnectExpired, ReasonBlocked))
	droppedBefore := counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total")

	passes := 0
	queue.setRetrier(func(key retryKey) {
		passes++
		for _, conn := range userConns {
			if queue.pending(conn) {
				continue
			}
			queue.requeueRetry(key, conn, centrifuge.DisconnectExpired, ReasonBlocked)
		}
	})
	for range 60 {
		select {
		case conn := <-queue.items:
			queue.begin(conn)
		default:
			t.Fatal("queue ran empty before 60 dequeues")
		}
	}

	require.Positive(t, passes)
	require.LessOrEqual(t, passes, 13)
	require.Equal(t, droppedBefore, counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total"))
}

// TestDisconnectQueueReleasesHeldKeyAtHalf drops a keyed disconnect from a full queue of 10. The first pass runs once the
// queue is at or below half, that is at the fifth dequeue, and it drops again, so the key is held. The held key comes back
// only when the queue is at or below half again, which is five dequeues later, not at the next dequeue.
func TestDisconnectQueueReleasesHeldKeyAtHalf(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	for i := range 10 {
		conn := &fakeConnection{id: "fill-" + strconv.Itoa(i)}
		require.True(t, queue.enqueueKeyed(testRetryKey, conn, centrifuge.DisconnectExpired, ReasonSession))
	}
	key := retryKey{user: "held-user"}
	require.False(t, queue.enqueueKeyed(key, &fakeConnection{id: "dropped"}, centrifuge.DisconnectExpired, ReasonBlocked))
	droppedBefore := counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total")

	passes := 0
	queue.setRetrier(func(retryKey) {
		passes++
		for i := range 100 {
			conn := &fakeConnection{id: "pass-" + strconv.Itoa(passes) + "-" + strconv.Itoa(i)}
			queue.requeueRetry(key, conn, centrifuge.DisconnectExpired, ReasonBlocked)
		}
	})
	dequeue := func() {
		select {
		case conn := <-queue.items:
			queue.begin(conn)
		default:
			t.Fatal("queue ran empty")
		}
	}

	for range 4 {
		dequeue()
	}
	require.Equal(t, 0, passes, "no pass while the queue is above half")
	dequeue()
	require.Equal(t, 1, passes, "the first pass runs at half")
	require.Len(t, queue.items, 10, "the first pass fills the queue again and drops")

	for range 4 {
		dequeue()
	}
	require.Equal(t, 1, passes, "a held key does not come back while the queue is above half")
	dequeue()
	require.Equal(t, 2, passes, "the held key comes back at half")
	require.Equal(t, droppedBefore, counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total"))
}

// TestRetryKeysAreNotCapped drops disconnects under 10000 distinct keys while the queue holds one. Every key is kept, and
// once the queue is drained each key is retried exactly once, so no pass runs over the whole node.
func TestRetryKeysAreNotCapped(t *testing.T) {
	queue := NewDisconnectQueue(10000, 1)
	queued := &fakeConnection{id: "queued"}
	require.True(t, queue.enqueueKeyed(testRetryKey, queued, centrifuge.DisconnectExpired, ReasonSession))
	for i := range 10000 {
		key := retryKey{user: strconv.Itoa(i)}
		require.False(t, queue.enqueueKeyed(key, &fakeConnection{id: "dropped"}, centrifuge.DisconnectExpired, ReasonSession))
	}
	queue.mu.Lock()
	require.Len(t, queue.retry, 10000)
	queue.mu.Unlock()

	retried := 0
	queue.setRetrier(func(retryKey) {
		retried++
	})
	<-queue.items
	queue.begin(queued)

	require.Equal(t, 10000, retried)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Empty(t, queue.retry)
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
