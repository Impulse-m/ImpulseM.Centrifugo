package accessrevoke

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	retriesMetric     = "centrifugo_access_revoke_disconnect_retries_total"
	disconnectsMetric = "centrifugo_access_revoke_disconnects_total"
)

// labeledCounterValue returns the value of the counter of the test registry which has the label value.
func labeledCounterValue(t *testing.T, name string, labelName string, labelValue string) float64 {
	t.Helper()
	families, err := testRegistry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// newDroppingRevoker returns a revoker whose queue holds one disconnect. The queue does not run until start is called,
// so the second disconnect of a revocation is dropped.
func newDroppingRevoker(t *testing.T, hub Hub, store *Store) (*Revoker, func()) {
	t.Helper()
	queue := NewDisconnectQueue(10000, 1)
	revoker := NewRevoker(store, hub, queue)
	start := func() {
		cancel, errCh := runQueue(t, queue)
		t.Cleanup(func() {
			cancel()
			<-errCh
		})
	}
	return revoker, start
}

// TestRevokeSessionDroppedDisconnectIsRetried revokes the session of two connections of a user while the queue has room
// for one. The dropped disconnect is retried by user once the queue runs.
func TestRevokeSessionDroppedDisconnectIsRetried(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	connectTestClient(t, node, "42|s1|0|1000")
	connectTestClient(t, node, "42|s1|0|1000")
	waitConnections(t, node, "42", 2)
	retries := labeledCounterValue(t, retriesMetric, "kind", retryKindUser)

	require.NoError(t, revoker.RevokeSession("42", "s1", 0, 0, nil))
	start()

	waitConnections(t, node, "42", 0)
	require.Equal(t, retries+1, labeledCounterValue(t, retriesMetric, "kind", retryKindUser))
}

// TestBlockUserDroppedDisconnectIsRetried blocks a user whose two connections do not fit in the queue. Both are
// disconnected once the queue runs.
func TestBlockUserDroppedDisconnectIsRetried(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-1", 2)

	require.NoError(t, revoker.BlockUser("user-1", 0))
	start()

	waitConnections(t, node, "user-1", 0)
}

// TestInvalidateUserDroppedDisconnectIsRetried invalidates the tokens of two connections of a user whose disconnects do
// not fit in the queue. Both are disconnected once the queue runs.
func TestInvalidateUserDroppedDisconnectIsRetried(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	connectTestClient(t, node, "user-3|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-3|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-3", 2)

	require.NoError(t, revoker.InvalidateUser("user-3", 5, 200, 0, 0))
	start()

	waitConnections(t, node, "user-3", 0)
}

// TestSessionIndexDroppedDisconnectIsRetried revokes a session of two connections without user while the queue has room
// for one. The dropped disconnect is retried by session once the queue runs.
func TestSessionIndexDroppedDisconnectIsRetried(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	session, _ := Key("s1")
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	var firstTracked Tracked
	var secondTracked Tracked
	store.SessionConnections().Track(first, &firstTracked, session)
	store.SessionConnections().Track(second, &secondTracked, session)

	require.NoError(t, revoker.RevokeSession("", "s1", 0, 0, nil))
	start()

	require.Eventually(t, func() bool {
		firstDisconnects, _ := first.snapshot()
		secondDisconnects, _ := second.snapshot()
		return len(firstDisconnects) == 1 && len(secondDisconnects) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		firstDisconnects, _ := first.snapshot()
		secondDisconnects, _ := second.snapshot()
		return len(firstDisconnects) > 1 || len(secondDisconnects) > 1
	}, 200*time.Millisecond, 5*time.Millisecond)
}

// TestRetryIgnoresExpiredRevocation drops a session disconnect and lets the revocation expire before the queue runs. The
// retry reads the store, finds no revocation, and disconnects nothing more.
func TestRetryIgnoresExpiredRevocation(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	now := int64(500)
	store.nowUnix = func() int64 {
		return now
	}
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	session, _ := Key("s1")
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	var firstTracked Tracked
	var secondTracked Tracked
	store.SessionConnections().Track(first, &firstTracked, session)
	store.SessionConnections().Track(second, &secondTracked, session)

	require.NoError(t, revoker.RevokeSession("", "s1", 0, 1000, nil))
	now = 2000
	start()

	// The index yields the connections in no fixed order, so either one is the one the queue holds; the other is the
	// dropped one, which the expired revocation must not disconnect.
	disconnected := func() int {
		firstDisconnects, _ := first.snapshot()
		secondDisconnects, _ := second.snapshot()
		return len(firstDisconnects) + len(secondDisconnects)
	}
	require.Eventually(t, func() bool {
		return disconnected() == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		return disconnected() > 1
	}, 200*time.Millisecond, 10*time.Millisecond)
}

// TestRetryEnqueuesEachConnectionOnce blocks a user with 50 connections while the queue holds 10. The retries enqueue
// the connections which did not fit, so each connection is disconnected exactly once.
func TestRetryEnqueuesEachConnectionOnce(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 10)
	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})
	revoker := NewRevoker(store, node.Hub(), queue)
	for i := range 50 {
		connectTestClient(t, node, fmt.Sprintf("user-1|s%d|0|100", i))
	}
	waitConnections(t, node, "user-1", 50)
	before := labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonBlocked))

	require.NoError(t, revoker.BlockUser("user-1", 0))

	waitConnections(t, node, "user-1", 0)
	require.Eventually(t, func() bool {
		return labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonBlocked)) == before+50
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		return labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonBlocked)) > before+50
	}, 100*time.Millisecond, 10*time.Millisecond)
}

// TestRevokeSessionWithRoomRequestsNoSweep revokes the session of a connection while the disconnect queue has room.
// The disconnect is queued, so no sweep is requested.
func TestRevokeSessionWithRoomRequestsNoSweep(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	revoker := NewRevoker(store, node.Hub(), queue)
	swept := make(chan struct{}, 1)
	revoker.sweep = func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}
	connectTestClient(t, node, "42|s1|0|1000")

	require.NoError(t, revoker.RevokeSession("42", "s1", 1000, 0, nil))
	select {
	case <-swept:
		t.Fatal("a queued disconnect requested a sweep")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestRetryDoesNotRequeueClosingConnection drops the session disconnects of three indexed connections while the queue
// holds one. Each retry pass finds the connections which are closing and skips them, so each connection is
// disconnected exactly once, and the session is retried once per pass that found a drop.
func TestRetryDoesNotRequeueClosingConnection(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker, start := newDroppingRevoker(t, node.Hub(), store)
	session, _ := Key("s1")
	conns := make([]*fakeConnection, 0, 3)
	for i := range 3 {
		conn := &fakeConnection{id: strconv.Itoa(i)}
		var tracked Tracked
		store.SessionConnections().Track(conn, &tracked, session)
		conns = append(conns, conn)
	}
	retries := labeledCounterValue(t, retriesMetric, "kind", retryKindSession)

	require.NoError(t, revoker.RevokeSession("", "s1", 0, 0, nil))
	start()

	require.Eventually(t, func() bool {
		return labeledCounterValue(t, retriesMetric, "kind", retryKindSession) == retries+2
	}, 5*time.Second, 5*time.Millisecond)
	for _, conn := range conns {
		require.Eventually(t, func() bool {
			disconnects, _ := conn.snapshot()
			return len(disconnects) == 1
		}, 5*time.Second, 5*time.Millisecond)
	}
	require.Never(t, func() bool {
		for _, conn := range conns {
			disconnects, _ := conn.snapshot()
			if len(disconnects) > 1 {
				return true
			}
		}
		return false
	}, 200*time.Millisecond, 5*time.Millisecond)
}

// TestRevokeTwiceDisconnectsOnce blocks a user and revokes the session of its connection at once. The connection is
// disconnected once: the second revocation finds it queued or closing.
func TestRevokeTwiceDisconnectsOnce(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)
	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	count := func() float64 {
		return labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonBlocked)) +
			labeledCounterValue(t, disconnectsMetric, "reason", string(ReasonSession))
	}
	before := count()

	require.NoError(t, revoker.BlockUser("user-1", 0))
	require.NoError(t, revoker.RevokeSession("user-1", testUUID, 0, 0, nil))

	waitConnections(t, node, "user-1", 0)
	require.Eventually(t, func() bool {
		return count() == before+1
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		return count() > before+1
	}, 100*time.Millisecond, 5*time.Millisecond)
}
