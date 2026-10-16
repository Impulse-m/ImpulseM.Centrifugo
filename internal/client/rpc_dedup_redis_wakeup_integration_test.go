//go:build integration

package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// dedupRegistryWithRedisWakeup returns the dedup registry of dedupRegistryWithRedis, with the wakeup on and the default
// cap of the waiter nodes.
func dedupRegistryWithRedisWakeup(prefix string) configtypes.RPCRegistry {
	reg := dedupRegistryWithRedis(prefix)
	reg.Dedup.Redis.Wakeup = configtypes.RPCDedupWakeup{
		Enabled:        true,
		MaxWaiterNodes: 64,
	}
	return reg
}

// dedupRegistryWithRedisWakeupCap returns dedupRegistryWithRedisWakeup with the cap of the waiter nodes at maxWaiterNodes.
func dedupRegistryWithRedisWakeupCap(prefix string, maxWaiterNodes int) configtypes.RPCRegistry {
	reg := dedupRegistryWithRedisWakeup(prefix)
	reg.Dedup.Redis.Wakeup.MaxWaiterNodes = maxWaiterNodes
	return reg
}

// wakeupMetrics are the series of the wakeup which a test reads.
type wakeupMetrics struct {
	reads         prometheus.Counter
	waits         *prometheus.CounterVec
	notifications *prometheus.CounterVec
	wakes         *prometheus.CounterVec
	watchers      prometheus.Gauge
}

// testWakeupMetrics sets the wakeup series of the dedup metrics for the test, before its handlers are built. The
// previous series are restored when the test ends.
func testWakeupMetrics(t *testing.T) wakeupMetrics {
	t.Helper()
	m := wakeupMetrics{
		reads:         prometheus.NewCounter(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_remote_reads_total"}),
		waits:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_remote_waits_total"}, []string{"end"}),
		notifications: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_notifications_total"}, []string{"op", "result"}),
		wakes:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_wakes_total"}, []string{"result"}),
		watchers:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_rpc_registry_dedup_wake_watchers"}),
	}
	previousReads := metrics.RPCRegistryDedupRemoteReadsTotal
	previousWaits := metrics.RPCRegistryDedupRemoteWaitsTotal
	previousNotifications := metrics.RPCRegistryDedupNotificationsTotal
	previousWakes := metrics.RPCRegistryDedupWakesTotal
	previousWatchers := metrics.RPCRegistryDedupWakeWatchers
	metrics.RPCRegistryDedupRemoteReadsTotal = m.reads
	metrics.RPCRegistryDedupRemoteWaitsTotal = m.waits
	metrics.RPCRegistryDedupNotificationsTotal = m.notifications
	metrics.RPCRegistryDedupWakesTotal = m.wakes
	metrics.RPCRegistryDedupWakeWatchers = m.watchers
	t.Cleanup(func() {
		metrics.RPCRegistryDedupRemoteReadsTotal = previousReads
		metrics.RPCRegistryDedupRemoteWaitsTotal = previousWaits
		metrics.RPCRegistryDedupNotificationsTotal = previousNotifications
		metrics.RPCRegistryDedupWakesTotal = previousWakes
		metrics.RPCRegistryDedupWakeWatchers = previousWatchers
	})
	return m
}

// startHandlerServices runs the services of h until the test ends, as the node does: the shared tier flushes its acks,
// and its wake watchers run while it has remote waits.
func startHandlerServices(t *testing.T, h *Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, len(h.Services()))
	services := h.Services()
	for _, s := range services {
		go func(s service.Service) {
			_ = s.Run(ctx)
			done <- struct{}{}
		}(s)
	}
	t.Cleanup(func() {
		cancel()
		for range services {
			<-done
		}
	})
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

// blockedXReadClients returns the clients of the integration Redis which block on an XREAD, as CLIENT LIST shows them.
func blockedXReadClients(t *testing.T) int {
	t.Helper()
	shard := integrationShard(t)
	list, err := shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
		return c.Do(context.Background(), c.B().ClientList().Build())
	}).ToString()
	require.NoError(t, err)
	blocked := 0
	for _, line := range strings.Split(list, "\n") {
		var isXRead, isBlocked bool
		for _, field := range strings.Fields(line) {
			if field == "cmd=xread" {
				isXRead = true
			}
			if strings.HasPrefix(field, "flags=") && strings.Contains(field, "b") {
				isBlocked = true
			}
		}
		if isXRead && isBlocked {
			blocked++
		}
	}
	return blocked
}

// requireNoBlockedXRead waits until no client of the integration Redis blocks on an XREAD. A connection which a test
// cancelled may be listed for a moment after its close, so the check allows a short settle time; a wake watcher which
// idles would stay blocked and fail it.
func requireNoBlockedXRead(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return blockedXReadClients(t) == 0
	}, 2*time.Second, 10*time.Millisecond, "a wake watcher holds a blocked XREAD")
}

// A duplicate which waits for a call on another node is woken through the wake stream of its node when the call is
// written, and answers without reading the record again.
func TestRPCDedupRedisRemoteJoinWakesThroughStream(t *testing.T) {
	vec := testDedupOutcomes(t)
	m := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedisWakeup(prefix)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	startHandlerServices(t, nodeA)
	startHandlerServices(t, nodeB)
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
	// node-b is listed as a waiter in the record of node-a, and its wake watcher runs.
	waiters, found := integrationWaiters(t, prefix, key)
	require.True(t, found)
	require.Equal(t, nodeB.node.ID(), waiters)
	require.Equal(t, 1.0, testutil.ToFloat64(m.watchers))

	released := time.Now()
	backend.releaseGate()
	r1 := waitDedupResult(t, owner)
	require.NoError(t, r1.err)
	r2 := waitDedupResult(t, duplicate)
	t.Logf("wait latency with the wake stream: %s", time.Since(released))
	require.NoError(t, r2.err)
	require.Equal(t, []byte("reply:gate"), r2.reply.Data)
	require.True(t, r2.reply.Cached)
	require.Len(t, backend.Calls(), 1)

	require.Equal(t, 1.0, testutil.ToFloat64(m.reads))
	require.Equal(t, 1.0, testutil.ToFloat64(m.waits.WithLabelValues(rpcdedup.WatchEndWoken)))
	require.Equal(t, 1.0, testutil.ToFloat64(m.wakes.WithLabelValues("ok")))
}

// A duplicate beyond the cap of the waiter nodes is answered with the later error at once, and counted as refused.
func TestRPCDedupRedisRemoteJoinFullIsLater(t *testing.T) {
	vec := testDedupOutcomes(t)
	m := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedisWakeupCap(prefix, 1)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	nodeC := newRedisDedupHandler(t, reg, backend)
	startHandlerServices(t, nodeA)
	startHandlerServices(t, nodeB)
	startHandlerServices(t, nodeC)
	msgID := dedupMsgID(26)

	owner := dedupCallAsync(nodeA, newDedupClient("a1", "42", nil, testInstance), "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinB := testJoinSignal(nodeB)
	duplicateB := dedupCallAsync(nodeB, newDedupClient("b1", "42", nil, testInstance), "users.get", msgID, "gate")
	requireJoined(t, joinB)

	refused := dedupCallAsyncResult(t, nodeC, newDedupClient("c1", "42", nil, testInstance), "users.get", msgID, "gate")
	requireRetrySignal(t, refused.err, rpcRetryLater)
	require.Equal(t, 1.0, testutil.ToFloat64(m.waits.WithLabelValues(rpcdedup.WatchEndRefused)))
	// The refused duplicate is counted too: the record was in flight on another node.
	require.Equal(t, 2.0, testDedupCount(vec, "hit_inflight_remote"))

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
	require.NoError(t, waitDedupResult(t, duplicateB).err)
	require.Len(t, backend.Calls(), 1)
}

// With the wakeup off, a remote wait starts no wake watcher, and the record of the call holds no waiter.
func TestRPCDedupRedisWakeupOffStartsNoWatcher(t *testing.T) {
	vec := testDedupOutcomes(t)
	m := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedis(prefix)
	nodeA := newRedisDedupHandler(t, reg, backend)
	nodeB := newRedisDedupHandler(t, reg, backend)
	startHandlerServices(t, nodeA)
	startHandlerServices(t, nodeB)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(27)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: msgID}

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(nodeB)
	duplicate := dedupCallAsync(nodeB, clientB, "users.get", msgID, "gate")
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_remote"))
	_, found := integrationWaiters(t, prefix, key)
	require.False(t, found, "a begin without the wakeup registers no waiter")
	require.Equal(t, 0.0, testutil.ToFloat64(m.watchers))
	requireNoBlockedXRead(t)

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
	require.NoError(t, waitDedupResult(t, duplicate).err)
}

// With the wakeup on and no remote wait, a node runs no wake watcher and holds no blocked read.
func TestRPCDedupRedisWakeupIdleStartsNoWatcher(t *testing.T) {
	m := testWakeupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	h := newRedisDedupHandler(t, dedupRegistryWithRedisWakeup(prefix), backend)
	startHandlerServices(t, h)

	require.Equal(t, 0.0, testutil.ToFloat64(m.watchers))
	requireNoBlockedXRead(t)
}

// The handlers of the node carry the cancel notification only: the wakes travel through the wake streams.
func TestRPCDedupRedisNotificationHandlersAreCancelOnly(t *testing.T) {
	backend := newDedupBackend(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	h := newRedisDedupHandler(t, dedupRegistryWithRedisWakeup(prefix), backend)
	handlers := h.NotificationHandlers()
	require.Len(t, handlers, 1)
	require.Contains(t, handlers, rpcdedup.CancelNotificationOp)
}
