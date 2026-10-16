//go:build integration

package rpcdedup

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitObserver counts the polls and the ends of the waits of a node, and the other observed events.
type waitObserver struct {
	mu    sync.Mutex
	polls int
	ends  map[string]int
}

// AckDropped implements SharedObserver.
func (o *waitObserver) AckDropped() {}

// LostOwnership implements SharedObserver.
func (o *waitObserver) LostOwnership() {}

// WatchPolled implements SharedObserver.
func (o *waitObserver) WatchPolled() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.polls++
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

// counts returns the polls and the ends of the waits so far.
func (o *waitObserver) counts() (int, map[string]int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ends := make(map[string]int, len(o.ends))
	for end, n := range o.ends {
		ends[end] = n
	}
	return o.polls, ends
}

// newIntegrationWakeupNode returns a node like newIntegrationNode, whose waiters are registered and notified. Its done
// notifications are recorded in doneNotified, and deliverDone passes them to the waiting node, as the broker would.
// The node counts its waits in observer.
func newIntegrationWakeupNode(t *testing.T, prefix string, nodeID string, pollInterval time.Duration) *integrationNode {
	t.Helper()
	local, _ := newTestMemory(1024)
	connector := newIntegrationConnector(t, prefix)
	node := &integrationNode{local: local, connector: connector, observer: &waitObserver{}}
	node.shared = NewShared(SharedOptions{
		Local:         local,
		Backend:       NewRedis(prefix, connector, 0, nil),
		NodeID:        nodeID,
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
		Notifier: func(owner string, key Key) {
			node.notified = append(node.notified, notifyCall{nodeID: owner, key: key})
		},
		DoneNotifier: func(waiter string, key Key) {
			node.doneNotified = append(node.doneNotified, notifyCall{nodeID: waiter, key: key})
		},
		Observer:           node.observer,
		PollInitial:        time.Millisecond,
		PollMax:            5 * time.Millisecond,
		WakeupPollInterval: pollInterval,
		AckFlushEvery:      5 * time.Millisecond,
	})
	return node
}

// deliverDone passes the done notifications which from sent to the node to, as the broker of the nodes would.
func deliverDone(from *integrationNode, to *integrationNode) {
	for _, call := range from.doneNotified {
		if call.nodeID == to.shared.nodeID {
			to.shared.Wake(call.key)
		}
	}
}

func TestSharedTwoNodesWakeupCompletesWithoutPolling(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour)
	key := Key{Scope: testScope("u1", "a"), MsgID: 30}
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
	require.Equal(t, []notifyCall{{nodeID: "node-b", key: key}}, nodeA.doneNotified)
	deliverDone(nodeA, nodeB)
	waitDone(t, duplicate)
	require.Equal(t, []byte("answer"), duplicate.Record().Reply)

	polls, ends := nodeB.observer.counts()
	require.Equal(t, 1, polls)
	require.Equal(t, 1, ends[WatchEndNotified])
}

func TestSharedTwoNodesWakeupLostNotificationFallsBack(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", 20*time.Millisecond)
	key := Key{Scope: testScope("u1", "a"), MsgID: 31}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	duplicate := NewCallFlight(fingerprint, time.Second)
	_, err = nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)

	// Node-a completes, and the notification is lost: node-b's fallback poll finds the answer.
	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Reply: []byte("answer")}))
	waitDone(t, duplicate)
	require.Equal(t, []byte("answer"), duplicate.Record().Reply)
	_, ends := nodeB.observer.counts()
	require.Equal(t, 1, ends[WatchEndPolled])
}

func TestSharedTwoNodesWakeupOwnerCrash(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationNode(t, prefix, "node-a")
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", 20*time.Millisecond)
	key := Key{Scope: testScope("u1", "a"), MsgID: 32}
	fingerprint := Fingerprint("users.get", []byte("x"))

	_, err := nodeA.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	waiting := lookup.Flight

	// node-a died: no notification comes, and the record expires.
	deleteRedisKey(nodeA.connector, RedisKey(prefix, key))
	waitDone(t, waiting)
	require.True(t, waiting.Record().Abandoned)
	_, ends := nodeB.observer.counts()
	require.Equal(t, 1, ends[WatchEndPolled])

	lookup, err = nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
}

func TestSharedTwoNodesWakeupCancelThenDone(t *testing.T) {
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour)
	nodeB := newIntegrationWakeupNode(t, prefix, "node-b", time.Hour)
	key := Key{Scope: testScope("u1", "a"), MsgID: 33}
	fingerprint := Fingerprint("users.get", []byte("x"))

	owner := NewCallFlight(fingerprint, time.Second)
	_, err := nodeA.shared.Begin(key, owner)
	require.NoError(t, err)
	duplicate := NewCallFlight(fingerprint, time.Second)
	lookup, err := nodeB.shared.Begin(key, duplicate)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	// node-b cancels the call: the cancel reaches node-a by notification.
	require.NoError(t, nodeB.shared.Cancel(key, testCancelled))
	require.Equal(t, []notifyCall{{nodeID: "node-a", key: key}}, nodeB.notified)
	require.True(t, nodeA.shared.CancelFlight(key))

	// node-a's call ends with the cancelled error, and the done notification wakes node-b's wait with it.
	require.NoError(t, nodeA.shared.Complete(key, owner, Record{Err: testCancelled}))
	deliverDone(nodeA, nodeB)
	waitDone(t, duplicate)
	require.Equal(t, testCancelled, duplicate.Record().Err)
	_, ends := nodeB.observer.counts()
	require.Equal(t, 1, ends[WatchEndNotified])
}

// measureLiveOwner runs keys calls of node-a, each joined by node-b, and completes them after one second. It returns
// the polls which node-b's waits made, and how the waits ended.
func measureLiveOwner(t *testing.T, wakeup bool, keys int) (int, map[string]int) {
	t.Helper()
	prefix := uniqueRedisPrefix()
	nodeA := newIntegrationWakeupNode(t, prefix, "node-a", time.Hour)
	var nodeB *integrationNode
	if wakeup {
		nodeB = newIntegrationWakeupNode(t, prefix, "node-b", 2*time.Second)
	} else {
		nodeB = newIntegrationNodeObserved(t, prefix, "node-b")
	}
	fingerprint := Fingerprint("users.get", []byte("x"))
	owners := make([]*Flight, keys)
	waiting := make([]*Flight, keys)
	for i := range keys {
		key := Key{Scope: testScope("u1", "a"), MsgID: uint64(1000 + i)}
		owners[i] = NewCallFlight(fingerprint, time.Second)
		_, err := nodeA.shared.Begin(key, owners[i])
		require.NoError(t, err)
		lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
		require.NoError(t, err)
		require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
		waiting[i] = lookup.Flight
	}
	time.Sleep(time.Second)
	for i := range keys {
		key := Key{Scope: testScope("u1", "a"), MsgID: uint64(1000 + i)}
		require.NoError(t, nodeA.shared.Complete(key, owners[i], Record{Reply: []byte("answer")}))
	}
	deliverDone(nodeA, nodeB)
	for i := range keys {
		waitDone(t, waiting[i])
	}
	return nodeB.observer.counts()
}

// newIntegrationNodeObserved returns a node with the ramp of the defaults, which counts its waits in observer. It is the
// node of the measurements with the wakeup off, where the ramp is what a deployment runs.
func newIntegrationNodeObserved(t *testing.T, prefix string, nodeID string) *integrationNode {
	t.Helper()
	node := newIntegrationNode(t, prefix, nodeID)
	node.observer = &waitObserver{}
	node.shared.observer = node.observer
	node.shared.pollInitial = sharedDefaultPollInitial
	node.shared.pollMax = sharedDefaultPollMax
	return node
}

// TestSharedWakeupPollCount measures the polls of the waits of a live owner, with the wakeup off and on. With the
// wakeup on, each wait polls once and ends notified.
func TestSharedWakeupPollCount(t *testing.T) {
	const keys = 200
	offPolls, offEnds := measureLiveOwner(t, false, keys)
	t.Logf("live owner, wakeup off: %d keys, %d polls, %.2f polls per wait, ends %v", keys, offPolls, float64(offPolls)/keys, offEnds)

	onPolls, onEnds := measureLiveOwner(t, true, keys)
	t.Logf("live owner, wakeup on (2s): %d keys, %d polls, %.2f polls per wait, ends %v", keys, onPolls, float64(onPolls)/keys, onEnds)
	require.LessOrEqual(t, onPolls, 2*keys)
	require.Equal(t, keys, onEnds[WatchEndNotified])
	require.Zero(t, onEnds[WatchEndPolled])
}

// TestSharedWakeupDeadOwnerPollCount measures the polls of the waits whose owner dies after one second, with the
// wakeup off and on. The in-flight TTL is three seconds.
func TestSharedWakeupDeadOwnerPollCount(t *testing.T) {
	for _, wakeup := range []bool{false, true} {
		prefix := uniqueRedisPrefix()
		nodeA := newIntegrationNode(t, prefix, "node-a")
		var nodeB *integrationNode
		if wakeup {
			nodeB = newIntegrationWakeupNode(t, prefix, "node-b", time.Second)
		} else {
			nodeB = newIntegrationNodeObserved(t, prefix, "node-b")
		}
		nodeB.shared.inflightGrace = 2 * time.Second
		fingerprint := Fingerprint("users.get", []byte("x"))
		key := Key{Scope: testScope("u1", "a"), MsgID: 2000}
		_, err := nodeA.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
		require.NoError(t, err)
		lookup, err := nodeB.shared.Begin(key, NewCallFlight(fingerprint, time.Second))
		require.NoError(t, err)
		require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

		time.Sleep(time.Second)
		deleteRedisKey(nodeA.connector, RedisKey(prefix, key))
		waitDone(t, lookup.Flight)
		polls, ends := nodeB.observer.counts()
		t.Logf("dead owner, wakeup %t (interval %s): %d polls per wait, ends %v", wakeup, map[bool]string{false: "ramp", true: "1s"}[wakeup], polls, ends)
		require.Equal(t, 1, ends[WatchEndPolled]+ends[WatchEndDeadline]+ends[WatchEndNotified])
	}
}
