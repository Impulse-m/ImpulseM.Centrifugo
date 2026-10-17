//go:build integration

package rpcdedup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// waitObserver counts the reads and the ends of the waits of a node, and its wakes, and keeps the time of every read.
type waitObserver struct {
	mu        sync.Mutex
	reads     int
	readTimes []time.Time
	ends      map[string]int
	woke      [2]int
}

// AckDropped implements SharedObserver.
func (o *waitObserver) AckDropped() {}

// LostOwnership implements SharedObserver.
func (o *waitObserver) LostOwnership() {}

// WatchRead implements SharedObserver.
func (o *waitObserver) WatchRead() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reads++
	o.readTimes = append(o.readTimes, time.Now())
}

// WatchEnded implements SharedObserver.
func (o *waitObserver) WatchEnded(end string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ends == nil {
		o.ends = make(map[string]int)
	}
	o.ends[end]++
}

// Woke implements SharedObserver.
func (o *waitObserver) Woke(written int, failed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.woke[0] += written
	o.woke[1] += failed
}

// counts returns the reads and the ends of the waits so far.
func (o *waitObserver) counts() (int, map[string]int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ends := make(map[string]int, len(o.ends))
	for end, n := range o.ends {
		ends[end] = n
	}
	return o.reads, ends
}

// readTimesCopy returns the times of the reads so far.
func (o *waitObserver) readTimesCopy() []time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]time.Time(nil), o.readTimes...)
}

// newIntegrationWakeupNode returns a node like newIntegrationNode, with the wakeup on and its Run started. Its check is
// checkDelay after a wait starts, and its wake watcher stops idle after its last remote wait ended. Its blocking reads
// are 100 ms, so that an idle stop is prompt. Stop ends the Run of the node, and waits for it.
func newIntegrationWakeupNode(t *testing.T, prefix string, nodeID string, checkDelay time.Duration, idle time.Duration) *integrationNode {
	t.Helper()
	node := newIntegrationNode(t, prefix, nodeID)
	node.backend.readBlock = 100 * time.Millisecond
	node.shared.wakeup = true
	node.shared.checkDelay = checkDelay
	node.shared.watchIdleAfter = idle
	node.shared.observer = node.waits
	ctx, cancel := context.WithCancel(context.Background())
	node.stop = cancel
	node.done = make(chan struct{})
	go func() {
		defer close(node.done)
		_ = node.shared.Run(ctx)
	}()
	t.Cleanup(node.stopAndWait)
	return node
}

// stopAndWait ends the Run of the node, if it runs, and waits for it to return.
func (node *integrationNode) stopAndWait() {
	if node.stop == nil {
		return
	}
	node.stop()
	<-node.done
	node.stop = nil
}

// requireDoneWithin fails the test unless flight is finished within d.
func requireDoneWithin(t *testing.T, flight *Flight, d time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !flightOpen(flight)
	}, d, time.Millisecond)
}

// integrationRecordKey returns the key of the test with msgID.
func integrationRecordKey(msgID uint64) Key {
	return Key{Scope: testScope("u1", "a"), MsgID: msgID}
}

func TestSharedTwoNodesWakeupCompletesWithoutReadingTwice(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour, time.Hour)
	key := integrationRecordKey(30)
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	duplicate := NewCallFlight(fingerprint, time.Second)
	lookup, err := nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waiters, found := integrationField(t, nodeA.connector, redisKey, "w")
	require.True(t, found)
	require.Equal(t, "node-b", waiters)

	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))
	requireDoneWithin(t, duplicate, 200*time.Millisecond)
	require.Equal(t, []byte("answer"), duplicate.Record().Reply)

	reads, _ := nodeB.waits.counts()
	require.Equal(t, 1, reads)
	require.Eventually(t, func() bool {
		_, ends := nodeB.waits.counts()
		return ends[WatchEndWoken] == 1
	}, 2*time.Second, 5*time.Millisecond, "the wait is counted as woken")
}

func TestSharedTwoNodesWakeupLostWakeIsChecked(t *testing.T) {
	prefix := uniqueRedisPrefix()
	// The owner has the wakeup off, so it writes no wake: the waiter is answered at its check.
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", 500*time.Millisecond, time.Hour)
	key := integrationRecordKey(31)
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	duplicate := NewCallFlight(fingerprint, time.Second)
	_, err = nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)

	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))
	waitDone(t, duplicate)
	require.Equal(t, []byte("answer"), duplicate.Record().Reply)
	require.Eventually(t, func() bool {
		_, ends := nodeB.waits.counts()
		return ends[WatchEndChecked] == 1
	}, 2*time.Second, 5*time.Millisecond, "the wait is counted as checked")
}

func TestSharedTwoNodesWakeupOwnerCrash(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", 20*time.Millisecond, time.Hour)
	nodeB.shared.inflightGrace = 50 * time.Millisecond
	key := integrationRecordKey(32)
	fingerprint := Fingerprint("users.get", []byte("x"))

	_, err := nodeA.shared.Begin(key, NewCallFlight(fingerprint, 10*time.Millisecond))
	require.NoError(t, err)
	lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, 10*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waiting := lookup.Flight

	// node-a died: its record stays in flight, so the check and the read at the deadline both find it in flight.
	waitDone(t, waiting)
	require.True(t, waiting.Record().Abandoned)
	reads, _ := nodeB.waits.counts()
	require.Equal(t, 2, reads, "the check and the read at the deadline")
	require.Eventually(t, func() bool {
		_, ends := nodeB.waits.counts()
		return ends[WatchEndDeadline] == 1
	}, 2*time.Second, 5*time.Millisecond, "the wait is counted as ended at its deadline")

	// The record expires: a retransmit runs the call.
	deleteRedisKey(nodeA.connector, RedisKey(prefix, key))
	lookup, err = nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

func TestSharedTwoNodesWakeupCancelThenDone(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour, time.Hour)
	key := integrationRecordKey(33)
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	duplicate := NewCallFlight(fingerprint, time.Second)
	lookup, err := nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	// node-b cancels the call: the cancel reaches node-a by notification, from a goroutine of node-b.
	require.NoError(t, nodeB.shared.Cancel(key, testCancelled))
	require.Eventually(t, func() bool {
		return len(nodeB.notifiedCalls()) == 1
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, []notifyCall{{nodeID: "node-a", key: key}}, nodeB.notifiedCalls())
	require.True(t, nodeA.shared.CancelFlight(key))

	// node-a's call ends with the cancelled error, and the wake through the stream answers node-b's wait with it.
	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Err: testCancelled}))
	waitDone(t, duplicate)
	require.Equal(t, testCancelled, duplicate.Record().Err)
	require.Eventually(t, func() bool {
		_, ends := nodeB.waits.counts()
		return ends[WatchEndWoken] == 1
	}, 2*time.Second, 5*time.Millisecond, "the wait is counted as woken")
}

func TestSharedTwoNodesWakeupFullRefuses(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour, time.Hour)
	nodeC := newIntegrationWakeupNode(t, prefix, "node-c", time.Hour, time.Hour)
	nodeB.shared.maxWaiterNodes = 1
	nodeC.shared.maxWaiterNodes = 1
	key := integrationRecordKey(34)
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	waitingB := NewCallFlight(fingerprint, time.Second)
	lookup, err := nodeB.shared.Begin(key, waitingB)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	require.True(t, flightOpen(waitingB), "node-b is listed, so it waits")

	refused := NewCallFlight(fingerprint, time.Second)
	lookup, err = nodeC.shared.Begin(key, refused)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	require.False(t, flightOpen(refused), "node-c is beyond the cap, and is answered at once")
	require.True(t, refused.Record().Abandoned)
	require.True(t, refused.Record().NotRun)
	_, ends := nodeC.waits.counts()
	require.Equal(t, 1, ends[WatchEndRefused])

	waiters, _ := integrationField(t, nodeA.connector, redisKey, "w")
	require.Equal(t, "node-b", waiters)

	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))
	waitDone(t, waitingB)
	require.Equal(t, []byte("answer"), waitingB.Record().Reply)
}

func TestSharedWakeupNodeRestartLeavesExpiringStream(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	fingerprint := Fingerprint("users.get", []byte("x"))

	// node-b-old waits for the first call, and then stops, as a node which restarts does.
	oldB := newIntegrationWakeupNode(t, prefix, "node-b-old", time.Hour, time.Hour)
	first := integrationRecordKey(35)
	_, err := nodeA.shared.Begin(first, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	_, err = oldB.shared.Begin(first, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	oldB.stopAndWait()

	// The owner completes the first call: its wake goes to the stream of the old id, which nobody reads any more.
	require.NoError(t, nodeA.shared.Complete(first, nodeA.shared.local.flightOf(first), Record{Reply: []byte("answer")}))
	oldStream := WakeStreamKey(prefix, "node-b-old")
	require.Equal(t, int64(1), integrationStreamLen(t, nodeA.connector, oldStream))
	ttl, err := integrationDo(nodeA.connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Pttl().Key(oldStream).Build()
	}).AsInt64()
	require.NoError(t, err)
	require.Greater(t, ttl, int64(0))
	require.LessOrEqual(t, ttl, wakeStreamTTL.Milliseconds())

	// The restarted node has a new id, and its waits are woken through its own stream.
	newB := newIntegrationWakeupNode(t, prefix, "node-b-new", time.Hour, time.Hour)
	second := integrationRecordKey(36)
	_, err = nodeA.shared.Begin(second, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	waiting := NewCallFlight(fingerprint, time.Second)
	_, err = newB.shared.Begin(second, waiting)
	require.NoError(t, err)
	require.NoError(t, nodeA.shared.Complete(second, nodeA.shared.local.flightOf(second), Record{Reply: []byte("fresh")}))
	waitDone(t, waiting)
	require.Equal(t, []byte("fresh"), waiting.Record().Reply)
}

func TestSharedWakeupWatcherIdleStops(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour, 200*time.Millisecond)
	fingerprint := Fingerprint("users.get", []byte("x"))

	first := integrationRecordKey(37)
	_, err := nodeA.shared.Begin(first, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	waiting := NewCallFlight(fingerprint, time.Second)
	_, err = nodeB.shared.Begin(first, waiting)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return nodeB.backendObserver.watchers.Load() == 1
	}, time.Second, time.Millisecond, "the wait starts the watcher of the shard")
	require.NoError(t, nodeA.shared.Complete(first, nodeA.shared.local.flightOf(first), Record{Reply: []byte("answer")}))
	waitDone(t, waiting)

	// The node has no remote wait now: its watcher stops within the idle time and a read, and no blocked read is left.
	require.Eventually(t, func() bool {
		return nodeB.backendObserver.watchers.Load() == 0
	}, time.Second, 10*time.Millisecond, "the idle watcher stops")
	require.Eventually(t, func() bool {
		return integrationBlockedXReads(t, nodeB.connector) == 0
	}, time.Second, 10*time.Millisecond, "no blocked XREAD of the idle node is left")

	// A new wait starts the watcher again, and is woken.
	second := integrationRecordKey(38)
	_, err = nodeA.shared.Begin(second, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	again := NewCallFlight(fingerprint, time.Second)
	_, err = nodeB.shared.Begin(second, again)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return nodeB.backendObserver.watchers.Load() == 1
	}, time.Second, time.Millisecond)
	require.NoError(t, nodeA.shared.Complete(second, nodeA.shared.local.flightOf(second), Record{Reply: []byte("again")}))
	waitDone(t, again)
	require.Equal(t, []byte("again"), again.Record().Reply)
}

// measureLiveOwner runs keys calls of node-a, each joined by node-b, and completes them after one second. It returns
// the reads which node-b's waits made, and how the waits ended, and logs the reads per 250 ms bucket.
func measureLiveOwner(t *testing.T, wakeup bool, keys int) (int, map[string]int) {
	t.Helper()
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour, time.Hour)
	var nodeB *integrationNode
	if wakeup {
		// The check is an hour away: the completions of the keys take their own time, and the measure is the wakes alone.
		nodeB = newIntegrationWakeupNode(t, prefix, "node-b", time.Hour, time.Hour)
	} else {
		nodeB = newIntegrationNode(t, prefix, "node-b")
		nodeB.shared.observer = nodeB.waits
		nodeB.shared.pollInitial = sharedDefaultPollInitial
		nodeB.shared.pollMax = sharedDefaultPollMax
	}
	fingerprint := Fingerprint("users.get", []byte("x"))
	owners := make([]*Flight, keys)
	waiting := make([]*Flight, keys)
	start := time.Now()
	for i := range keys {
		key := integrationRecordKey(uint64(1000 + i))
		owners[i] = NewCallFlight(fingerprint, time.Second)
		_, err := nodeA.shared.Begin(key, owners[i])
		require.NoError(t, err)
		lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
		require.NoError(t, err)
		require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
		waiting[i] = lookup.Flight
	}
	// The owner completes one second after the first wait started, so that no check of an early wait comes before its
	// answer, however long the begins of the loop took.
	time.Sleep(time.Until(start.Add(time.Second)))
	for i := range keys {
		key := integrationRecordKey(uint64(1000 + i))
		require.NoError(t, nodeA.shared.Complete(key, owners[i], Record{Reply: []byte("answer")}))
	}
	for i := range keys {
		waitDone(t, waiting[i])
	}
	buckets := make(map[int]int)
	for _, at := range nodeB.waits.readTimesCopy() {
		buckets[int(at.Sub(start)/(250*time.Millisecond))]++
	}
	t.Logf("live owner, wakeup %t: %d keys, reads per 250 ms bucket %v", wakeup, keys, buckets)
	// Each wait is counted as ended after its flight is done, so the counts are waited for.
	require.Eventually(t, func() bool {
		_, ends := nodeB.waits.counts()
		total := 0
		for _, n := range ends {
			total += n
		}
		return total == keys
	}, 2*time.Second, 5*time.Millisecond, "every wait is counted as ended")
	return nodeB.waits.counts()
}

// TestSharedWakeupReadCount measures the reads of the waits of a live owner, with the wakeup off and on. With the wakeup
// on, each wait reads once, on its wake, and ends woken.
func TestSharedWakeupReadCount(t *testing.T) {
	const keys = 200
	offReads, offEnds := measureLiveOwner(t, false, keys)
	t.Logf("live owner, wakeup off: %d reads, %.2f reads per wait, ends %v", offReads, float64(offReads)/keys, offEnds)

	onReads, onEnds := measureLiveOwner(t, true, keys)
	t.Logf("live owner, wakeup on: %d reads, %.2f reads per wait, ends %v", onReads, float64(onReads)/keys, onEnds)
	require.LessOrEqual(t, onReads, keys)
	require.Equal(t, keys, onEnds[WatchEndWoken])
	require.Zero(t, onEnds[WatchEndChecked])
}

// TestSharedWakeupDeadOwnerReadCount measures the reads of the waits whose owner dies after one second, with the wakeup
// off and on. The in-flight TTL is three seconds, and the check of the wakeup is at 1.5 seconds, so with the wakeup on
// each wait reads at the check and once more at its deadline, where it ends.
func TestSharedWakeupDeadOwnerReadCount(t *testing.T) {
	const keys = 50
	for _, wakeup := range []bool{false, true} {
		prefix := uniqueRedisPrefix()
		nodeA := newIntegrationNode(t, prefix, "node-a")
		var nodeB *integrationNode
		if wakeup {
			nodeB = newIntegrationWakeupNode(t, prefix, "node-b", 1500*time.Millisecond, time.Hour)
		} else {
			nodeB = newIntegrationNode(t, prefix, "node-b")
			nodeB.shared.observer = nodeB.waits
			nodeB.shared.pollInitial = sharedDefaultPollInitial
			nodeB.shared.pollMax = sharedDefaultPollMax
		}
		nodeB.shared.inflightGrace = 2 * time.Second
		fingerprint := Fingerprint("users.get", []byte("x"))
		waiting := make([]*Flight, keys)
		for i := range keys {
			key := integrationRecordKey(uint64(2000 + i))
			_, err := nodeA.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
			require.NoError(t, err)
			lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
			require.NoError(t, err)
			require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
			waiting[i] = lookup.Flight
		}
		for i := range keys {
			waitDone(t, waiting[i])
		}
		require.Eventually(t, func() bool {
			_, ends := nodeB.waits.counts()
			return ends[WatchEndDeadline] == keys
		}, 2*time.Second, 5*time.Millisecond, "every wait is counted as ended at its deadline")
		reads, ends := nodeB.waits.counts()
		t.Logf("dead owner, wakeup %t: %d keys, %.2f reads per wait, ends %v", wakeup, keys, float64(reads)/keys, ends)
		if wakeup {
			require.Equal(t, 2*keys, reads, "two reads per wait, at the check and at the deadline")
			require.Equal(t, keys, ends[WatchEndDeadline])
		}
	}
}
