//go:build integration

package client

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// testRedisDedupConfig returns the shared tier config of exactly-once RPC for address and prefix.
func testRedisDedupConfig(address string, prefix string) configtypes.RPCDedupRedis {
	return configtypes.RPCDedupRedis{
		Enabled: true,
		RedisPrefixed: configtypes.RedisPrefixed{
			Prefix: prefix,
			Redis:  configtypes.Redis{Address: []string{address}},
		},
	}
}

// newDedupHandlerWithConfig returns a handler like newDedupHandler, whose config modify changes before the handler is
// built. modify may be nil.
func newDedupHandlerWithConfig(t testing.TB, reg configtypes.RPCRegistry, backend *dedupBackend, modify func(cfg *config.Config)) *Handler {
	t.Helper()
	registry := startDedupRegistry(t, reg, backend.conn(t))
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = reg
	if modify != nil {
		modify(&cfg)
	}
	h, _ := newRegistryTestHandler(t, cfg, registry)
	return h
}

// newRedisDedupHandler returns a handler of reg, whose shared tier is set up as Setup sets it up. Its connector is
// closed when the test ends.
func newRedisDedupHandler(t testing.TB, reg configtypes.RPCRegistry, backend *dedupBackend) *Handler {
	t.Helper()
	h := newDedupHandlerWithConfig(t, reg, backend, nil)
	require.NoError(t, h.setupRPCDedupRedis())
	require.NotNil(t, h.rpcDedup.connector)
	t.Cleanup(h.rpcDedup.connector.Close)
	return h
}

// testRedisDedupMetrics sets the Redis series of the dedup metrics for the test, before its handlers are built. It
// returns the error counter of the shared tier.
func testRedisDedupMetrics(t *testing.T) prometheus.Counter {
	t.Helper()
	errorsTotal := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_redis_errors_total"})
	previousErrors := metrics.RPCRegistryDedupRedisErrorsTotal
	metrics.RPCRegistryDedupRedisErrorsTotal = errorsTotal
	t.Cleanup(func() {
		metrics.RPCRegistryDedupRedisErrorsTotal = previousErrors
	})
	duration := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_rpc_registry_dedup_redis_duration_seconds"})
	previousDuration := metrics.RPCRegistryDedupRedisDuration
	metrics.RPCRegistryDedupRedisDuration = duration
	t.Cleanup(func() {
		metrics.RPCRegistryDedupRedisDuration = previousDuration
	})
	return errorsTotal
}

// integrationShard returns a shard of the integration Redis, closed when the test ends.
func integrationShard(t *testing.T) *redisshard.RedisShard {
	t.Helper()
	shards, err := redisshard.BuildRedisShards(configtypes.Redis{Address: []string{integrationRedisAddress}})
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, shard := range shards {
			shard.Close()
		}
	})
	return shards[0]
}

// scanRedisKeys returns the keys of shard which match pattern.
func scanRedisKeys(shard *redisshard.RedisShard, pattern string) []string {
	ctx := context.Background()
	var keys []string
	var cursor uint64
	for {
		entry, err := shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
			return c.Do(ctx, c.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build())
		}).AsScanEntry()
		if err != nil {
			return keys
		}
		keys = append(keys, entry.Elements...)
		cursor = entry.Cursor
		if cursor == 0 {
			return keys
		}
	}
}

// cleanupRedisPrefix deletes the keys of prefix when the test ends.
func cleanupRedisPrefix(t *testing.T, prefix string) {
	t.Helper()
	shard := integrationShard(t)
	t.Cleanup(func() {
		for _, key := range scanRedisKeys(shard, prefix+"*") {
			shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
				return c.Do(context.Background(), c.B().Del().Key(key).Build())
			})
		}
	})
}

func TestRPCDedupRedisRemoteInFlightJoin(t *testing.T) {
	vec := testDedupOutcomes(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	nodeA := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	nodeB := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(21)

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(nodeB)
	duplicate := dedupCallAsync(nodeB, clientB, "users.get", msgID, "gate")
	// The duplicate waits for the call of node-a: it is counted as a remote join.
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_remote"))
	backend.releaseGate()

	r1 := waitDedupResult(t, owner)
	r2 := waitDedupResult(t, duplicate)
	require.NoError(t, r1.err)
	require.NoError(t, r2.err)
	require.Equal(t, []byte("reply:gate"), r1.reply.Data)
	require.Equal(t, []byte("reply:gate"), r2.reply.Data)
	require.True(t, r2.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupRedisDoneHitAcrossNodes(t *testing.T) {
	vec := testDedupOutcomes(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	nodeA := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	nodeB := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(22)

	first := dedupCallAsyncResult(t, nodeA, clientA, "users.get", msgID, "x")
	require.NoError(t, first.err)
	second := dedupCallAsyncResult(t, nodeB, clientB, "users.get", msgID, "x")
	require.NoError(t, second.err)
	require.Equal(t, []byte("reply:x"), second.reply.Data)
	require.True(t, second.reply.Cached)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_done"))
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupRedisCancelAcrossNodes(t *testing.T) {
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	nodeA := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	nodeB := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	clientA := newDedupClient("a1", "42", nil, testInstance)
	clientB := newDedupClient("b1", "42", nil, testInstance)
	msgID := dedupMsgID(23)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: msgID}

	owner := dedupCallAsync(nodeA, clientA, "users.get", msgID, "gate")
	backend.waitEntered(t)
	reply, err := nodeB.OnRPC(clientB, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)

	// The test delivers node-b's cancel notification to node-a: the nodes of the test do not share a control broker.
	handler := nodeA.NotificationHandlers()[rpcdedup.CancelNotificationOp]
	require.NotNil(t, handler)
	handler(centrifuge.NotificationEvent{
		FromNodeID: nodeB.node.ID(),
		Op:         rpcdedup.CancelNotificationOp,
		Data:       rpcdedup.EncodeCancel(key),
	})
	requireCancelledError(t, waitDedupResult(t, owner).err)

	// node-b answers the retransmit from the record node-a kept in Redis.
	retransmit := dedupCallAsyncResult(t, nodeB, clientB, "users.get", msgID, "gate")
	requireCancelledError(t, retransmit.err)
	require.True(t, retransmit.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupRedisFailClosedWhenUnreachable(t *testing.T) {
	vec := testDedupOutcomes(t)
	errorsTotal := testRedisDedupMetrics(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.Redis = testRedisDedupConfig(unreachableRedisAddress(t), uniqueRedisPrefix())
	h := newRedisDedupHandler(t, reg, backend)
	client := newDedupClient("c1", "42", nil, testInstance)

	_, err := dedupCall(h, client, "users.get", dedupMsgID(31), "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(108), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Equal(t, 1.0, testDedupCount(vec, "store_error"))
	require.Equal(t, 1.0, testutil.ToFloat64(errorsTotal))
	require.Empty(t, backend.Calls())
}

func TestRPCDedupRedisFailOpenWhenUnreachable(t *testing.T) {
	vec := testDedupOutcomes(t)
	errorsTotal := testRedisDedupMetrics(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	reg.Dedup.Redis = testRedisDedupConfig(unreachableRedisAddress(t), uniqueRedisPrefix())
	h := newRedisDedupHandler(t, reg, backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(32)

	first := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	require.NoError(t, first.err)
	require.False(t, first.reply.Cached)
	require.Equal(t, 1.0, testDedupCount(vec, "miss"))
	require.Equal(t, 1.0, testutil.ToFloat64(errorsTotal))

	// The record is kept on this node: a retransmit is answered from it, and the call does not run again.
	retransmit := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	require.NoError(t, retransmit.err)
	require.True(t, retransmit.reply.Cached)
	require.Equal(t, []byte("reply:x"), retransmit.reply.Data)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupRedisFailOpenWhenConnectorIsClosed(t *testing.T) {
	vec := testDedupOutcomes(t)
	errorsTotal := testRedisDedupMetrics(t)
	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryWithRedis(prefix)
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	h := newRedisDedupHandler(t, reg, backend)
	h.rpcDedup.connector.Close()
	client := newDedupClient("c1", "42", nil, testInstance)

	r := dedupCallAsyncResult(t, h, client, "users.get", dedupMsgID(33), "x")
	require.NoError(t, r.err)
	require.False(t, r.reply.Cached)
	require.Equal(t, 1.0, testDedupCount(vec, "miss"))
	require.Equal(t, 1.0, testutil.ToFloat64(errorsTotal))
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupRedisReuseFromEngine(t *testing.T) {
	enginePrefix := uniqueRedisPrefix()
	shard := integrationShard(t)
	cleanupRedisPrefix(t, enginePrefix)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.Redis = configtypes.RPCDedupRedis{Enabled: true, ReuseFromEngine: true}
	h := newDedupHandlerWithConfig(t, reg, backend, func(cfg *config.Config) {
		cfg.Engine.Type = "redis"
		cfg.Engine.Redis.Prefix = enginePrefix
		cfg.Engine.Redis.Address = []string{integrationRedisAddress}
	})
	require.NoError(t, h.setupRPCDedupRedis())
	t.Cleanup(h.rpcDedup.connector.Close)
	client := newDedupClient("c1", "42", nil, testInstance)

	r := dedupCallAsyncResult(t, h, client, "users.get", dedupMsgID(34), "x")
	require.NoError(t, r.err)
	// The record is named by the engine prefix, not by the prefix of the dedup config.
	require.Len(t, scanRedisKeys(shard, enginePrefix+".rpc.*"), 1)
	require.Empty(t, scanRedisKeys(shard, "ignored"+enginePrefix+".rpc.*"))
}

func TestRPCDedupRedisServicesAndNotifications(t *testing.T) {
	backend := newDedupBackend(t)

	off := newDedupHandler(t, dedupRegistryConfig(), backend)
	require.Empty(t, off.Services())
	require.Empty(t, off.NotificationHandlers())

	prefix := uniqueRedisPrefix()
	cleanupRedisPrefix(t, prefix)
	h := newRedisDedupHandler(t, dedupRegistryWithRedis(prefix), backend)
	services := h.Services()
	require.Len(t, services, 2)
	require.Equal(t, h.rpcDedup.connector, services[0])
	require.Equal(t, h.rpcDedup.shared, services[1])
	require.Contains(t, h.NotificationHandlers(), rpcdedup.CancelNotificationOp)
}

// dedupRegistryWithRedis returns the dedup registry of the tests, with the shared tier at the integration Redis under
// prefix.
func dedupRegistryWithRedis(prefix string) configtypes.RPCRegistry {
	reg := dedupRegistryConfig()
	reg.Dedup.Redis = testRedisDedupConfig(integrationRedisAddress, prefix)
	return reg
}
