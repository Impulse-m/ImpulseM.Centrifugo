//go:build integration

package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// dedupRegistryWithRedisWakeup returns the dedup registry of dedupRegistryWithRedis, with the wakeup by notification
// on and pollInterval as its fallback poll interval.
func dedupRegistryWithRedisWakeup(prefix string, pollInterval time.Duration) configtypes.RPCRegistry {
	reg := dedupRegistryWithRedis(prefix)
	reg.Dedup.Redis.Notification = configtypes.RPCDedupNotification{
		Enabled:      true,
		PollInterval: configtypes.Duration(pollInterval),
	}
	return reg
}

// testWakeupMetrics sets the wakeup series of the dedup metrics for the test, before its handlers are built. It returns
// the poll counter, the waits by end and the notifications by op and result.
func testWakeupMetrics(t *testing.T) (prometheus.Counter, *prometheus.CounterVec, *prometheus.CounterVec) {
	t.Helper()
	polls := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_remote_polls_total"})
	waits := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_remote_waits_total"}, []string{"end"})
	notifications := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_notifications_total"}, []string{"op", "result"})
	previousPolls := metrics.RPCRegistryDedupRemotePollsTotal
	previousWaits := metrics.RPCRegistryDedupRemoteWaitsTotal
	previousNotifications := metrics.RPCRegistryDedupNotificationsTotal
	metrics.RPCRegistryDedupRemotePollsTotal = polls
	metrics.RPCRegistryDedupRemoteWaitsTotal = waits
	metrics.RPCRegistryDedupNotificationsTotal = notifications
	t.Cleanup(func() {
		metrics.RPCRegistryDedupRemotePollsTotal = previousPolls
		metrics.RPCRegistryDedupRemoteWaitsTotal = previousWaits
		metrics.RPCRegistryDedupNotificationsTotal = previousNotifications
	})
	return polls, waits, notifications
}

// integrationWaiters returns the waiters field of the in-flight record of key in the integration Redis, and false when
// the record has none.
func integrationWaiters(t *testing.T, prefix string, key rpcdedup.Key) (string, bool) {
	t.Helper()
	shard := integrationShard(t)
	res := shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
		return c.Do(context.Background(), c.B().Hget().Key(rpcdedup.RedisKey(prefix, key)).Field("w").Build())
	})
	value, err := res.ToString()
	if rueidis.IsRedisNil(err) {
		return "", false
	}
	require.NoError(t, err)
	return value, true
}

// A duplicate which waits for a call on another node is woken by the done notification of the node which runs the call,
// and answers without polling for the record.
func TestRPCDedupRedisRemoteJoinWakesByNotification(t *testing.T) {
	vec := testDedupOutcomes(t)
	polls, waits, notifications := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedisWakeup(prefix, time.Hour)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(25)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: msgID}

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(nodeB)
	duplicate := dedupCallAsync(nodeB, clientB, "users.get", msgID, "gate")
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_remote"))
	// node-b is registered as a waiter in the record of node-a.
	waiters, found := integrationWaiters(t, prefix, key)
	require.True(t, found)
	require.Equal(t, nodeB.node.ID(), waiters)

	handler := nodeB.NotificationHandlers()[rpcdedup.DoneNotificationOp]
	require.NotNil(t, handler)

	released := time.Now()
	backend.releaseGate()
	r1 := waitDedupResult(t, owner)
	require.NoError(t, r1.err)
	// The nodes of the test do not share a control broker, so the test delivers node-a's done notification to node-b.
	handler(centrifuge.NotificationEvent{
		FromNodeID: nodeA.node.ID(),
		Op:         rpcdedup.DoneNotificationOp,
		Data:       rpcdedup.EncodeDone(key),
	})
	r2 := waitDedupResult(t, duplicate)
	t.Logf("wait latency with the wakeup: %s", time.Since(released))
	require.NoError(t, r2.err)
	require.Equal(t, []byte("reply:gate"), r2.reply.Data)
	require.True(t, r2.reply.Cached)
	require.Len(t, backend.Calls(), 1)

	require.Equal(t, 1.0, testutil.ToFloat64(polls))
	require.Equal(t, 1.0, testutil.ToFloat64(waits.WithLabelValues(rpcdedup.WatchEndNotified)))
	// node-a could not send the notification here, which the metric counts as an error.
	require.Equal(t, 1.0, testutil.ToFloat64(notifications.WithLabelValues(metrics.RPCRegistryDedupNotifyDone, "error")))
}

// A duplicate which waits for a call on another node is answered by the fallback poll when its notification is lost.
func TestRPCDedupRedisRemoteJoinFallsBackWhenNotificationIsLost(t *testing.T) {
	vec := testDedupOutcomes(t)
	polls, waits, _ := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	// The shortest poll interval which config validation accepts.
	reg := dedupRegistryWithRedisWakeup(prefix, 100*time.Millisecond)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(26)

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(nodeB)
	duplicate := dedupCallAsync(nodeB, clientB, "users.get", msgID, "gate")
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_remote"))

	// Nothing is delivered: the duplicate gets its answer from the fallback poll.
	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
	r2 := waitDedupResult(t, duplicate)
	require.NoError(t, r2.err)
	require.Equal(t, []byte("reply:gate"), r2.reply.Data)
	require.True(t, r2.reply.Cached)
	require.Len(t, backend.Calls(), 1)

	require.Equal(t, 1.0, testutil.ToFloat64(waits.WithLabelValues(rpcdedup.WatchEndPolled)))
	require.GreaterOrEqual(t, testutil.ToFloat64(polls), 1.0)
}

// Without the wakeup, the handler has no done notification, and the record of a remote wait holds no waiter.
func TestRPCDedupRedisWakeupOffRegistersNoHandler(t *testing.T) {
	vec := testDedupOutcomes(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedis(prefix)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(27)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: msgID}

	handlers := nodeB.NotificationHandlers()
	require.Contains(t, handlers, rpcdedup.CancelNotificationOp)
	require.NotContains(t, handlers, rpcdedup.DoneNotificationOp)

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(nodeB)
	duplicate := dedupCallAsync(nodeB, clientB, "users.get", msgID, "gate")
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_remote"))
	_, found := integrationWaiters(t, prefix, key)
	require.False(t, found, "a begin without the wakeup registers no waiter")

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
	require.NoError(t, waitDedupResult(t, duplicate).err)
}

// With the wakeup on, the handlers of the node include the done notification.
func TestRPCDedupRedisWakeupRegistersDoneHandler(t *testing.T) {
	backend := newDedupBackend(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	h := newRedisDedupHandler(t, dedupRegistryWithRedisWakeup(prefix, time.Second), backend)
	require.Contains(t, h.NotificationHandlers(), rpcdedup.CancelNotificationOp)
	require.Contains(t, h.NotificationHandlers(), rpcdedup.DoneNotificationOp)
}
