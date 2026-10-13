package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// redisTestConfig returns redis_user_command limits with overrides, buckets of the default scope
// and of publish namespace overrides, an RPC method override and connect buckets.
func redisTestConfig() configtypes.RateLimitRedisUserCommand {
	return configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Default: testContainer(time.Second, 10),
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 5),
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Second, 20)},
				{NamespaceName: "notif", Enabled: true},
				{NamespaceName: "off", Enabled: false, Buckets: testList(time.Second, 1)},
			},
		},
		RPC: configtypes.RateLimitRPCBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 4),
			MethodOverrides: configtypes.RateLimitMethodOverrides{
				{Method: "slow", Enabled: true, Buckets: testList(20*time.Second, 1)},
				{Method: "empty", Enabled: true},
			},
		},
		Connect: testContainer(time.Minute, 3),
	}
}

func TestRedisPlanScopeNames(t *testing.T) {
	p := newRedisUserPlan(redisTestConfig())

	tests := []struct {
		name string
		op   Operation
		key  string
		want string
	}{
		{name: "publish base", op: OperationPublish, key: "", want: "publish"},
		{name: "publish namespace override", op: OperationPublish, key: "chat", want: "publish.ns.chat"},
		{name: "publish enabled override without buckets uses base", op: OperationPublish, key: "notif", want: "publish"},
		{name: "publish disabled override is ignored", op: OperationPublish, key: "off", want: "publish"},
		{name: "publish unknown namespace uses base", op: OperationPublish, key: "other", want: "publish"},
		{name: "history not configured uses default", op: OperationHistory, key: "", want: defaultScopeName},
		{name: "presence not configured uses default", op: OperationPresence, key: "chat", want: defaultScopeName},
		{name: "refresh not configured uses default", op: OperationRefresh, key: "", want: defaultScopeName},
		{name: "rpc base", op: OperationRPC, key: "", want: "rpc"},
		{name: "rpc method override", op: OperationRPC, key: "slow", want: "rpc.m.slow"},
		{name: "rpc enabled override without buckets uses base", op: OperationRPC, key: "empty", want: "rpc"},
		{name: "connect", op: OperationConnect, key: "", want: "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := p.ops[tt.op].resolve(tt.key)
			require.NotNil(t, s)
			require.Equal(t, tt.want, s.name)
		})
	}
}

func TestRedisPlanNoLimitWithoutBuckets(t *testing.T) {
	p := newRedisUserPlan(configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 1)},
	})
	require.Nil(t, p.ops[OperationHistory].resolve(""))
	require.Nil(t, p.ops[OperationConnect].resolve(""))
	require.False(t, p.ops[OperationPublish].needsKey())
}

func TestRedisPlanConnectFallsBackToDefault(t *testing.T) {
	p := newRedisUserPlan(configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Default: testContainer(time.Second, 10),
	})
	s := p.ops[OperationConnect].base
	require.NotNil(t, s)
	require.Equal(t, defaultScopeName, s.name)
}

func TestRedisPlanDefaultScopeIsShared(t *testing.T) {
	p := newRedisUserPlan(configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Default: testContainer(time.Second, 10),
	})
	history := p.ops[OperationHistory].resolve("")
	presence := p.ops[OperationPresence].resolve("")
	require.Same(t, history, presence)
}

func TestRedisScriptArgs(t *testing.T) {
	// Interval 1500ms with rate 10: emission 150ms, burst tolerance 1500ms, TTL 1500ms.
	args := redisScriptArgs([]slotBucket{
		{bucket: NewBucket(1500*time.Millisecond, 10)},
		{bucket: NewBucket(time.Second, 4)},
	}, 1500*time.Millisecond)
	require.Equal(t, []string{"1500", "2", "150000", "1500000", "250000", "1000000"}, args)

	// Sub-microsecond emission is raised to one microsecond, tolerance is never below emission.
	args = redisScriptArgs([]slotBucket{
		{bucket: NewBucket(time.Millisecond, 1000000)},
	}, time.Millisecond)
	require.Equal(t, []string{"1", "1", "1", "1000"}, args)

	// TTL is rounded up to milliseconds.
	args = redisScriptArgs([]slotBucket{{bucket: NewBucket(1500*time.Microsecond, 1)}}, 1500*time.Microsecond)
	require.Equal(t, "2", args[0])
}

func TestRedisKeyFormat(t *testing.T) {
	require.Equal(t, "centrifugo.rl.{42}.publish", redisKey("centrifugo", "42", "publish"))
	require.Equal(t, "app.rl.{a:b}.publish.ns.chat", redisKey("app", "a:b", "publish.ns.chat"))
}

func TestJumpConsistentHashSingleBucket(t *testing.T) {
	for i := 0; i < 1000; i++ {
		require.Equal(t, int32(0), jumpConsistentHash(fnv1a64(fmt.Sprint("user-", i)), 1))
	}
}

func TestShardIndexDistribution(t *testing.T) {
	const numUsers = 30000
	const numShards = 3
	counts := make([]int, numShards)
	for i := 0; i < numUsers; i++ {
		idx := shardIndex(fmt.Sprint("user-", i), numShards)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, numShards)
		counts[idx]++
	}
	expected := float64(numUsers) / numShards
	for _, c := range counts {
		require.InDelta(t, expected, float64(c), expected*0.1)
	}
}

func TestShardIndexStableAndMinimalMovement(t *testing.T) {
	const numUsers = 20000
	moved := 0
	for i := 0; i < numUsers; i++ {
		userID := fmt.Sprint("user-", i)
		before := shardIndex(userID, 3)
		require.Equal(t, before, shardIndex(userID, 3))
		after := shardIndex(userID, 4)
		if before != after {
			// Jump hash moves a key only to the new bucket.
			require.Equal(t, 3, after)
			moved++
		}
	}
	// About a quarter of the users move to the new shard.
	require.InDelta(t, numUsers/4, moved, numUsers*0.03)
}

func TestFnv1a64(t *testing.T) {
	// Reference values of FNV-1a 64.
	require.Equal(t, uint64(0xcbf29ce484222325), fnv1a64(""))
	require.Equal(t, uint64(0xaf63dc4c8601ec8c), fnv1a64("a"))
}

// fakeRedisRunner returns a runner which records calls and returns allowed and err.
func fakeRedisRunner(allowed bool, err error, calls *[]string) redisScriptRunner {
	return func(_ context.Context, _ *redisshard.RedisShard, key string, _ []string) (bool, error) {
		*calls = append(*calls, key)
		return allowed, err
	}
}

// fakeShardConfigs returns n shard configs. They are never dialed by tests, which replace connect.
func fakeShardConfigs(n int) []redisshard.RedisShardConfig {
	configs := make([]redisshard.RedisShardConfig, 0, n)
	for i := 0; i < n; i++ {
		configs = append(configs, redisshard.RedisShardConfig{Address: fmt.Sprintf("fake-%d:6379", i)})
	}
	return configs
}

// flakyConnector is a connect replacement which fails its first failFor calls, then succeeds.
type flakyConnector struct {
	calls   atomic.Int32
	failFor int32
}

func (f *flakyConnector) connect(configs []redisshard.RedisShardConfig) ([]*redisshard.RedisShard, error) {
	if n := f.calls.Add(1); n <= f.failFor {
		return nil, errors.New("connection refused")
	}
	return make([]*redisshard.RedisShard, len(configs)), nil
}

// newFakeRedisLimiter returns a connected limiter with two fake shards and the given runner.
func newFakeRedisLimiter(cfg configtypes.RateLimitRedisUserCommand, run redisScriptRunner) *RedisUserLimiter {
	connector := &flakyConnector{}
	l := NewRedisUserLimiter(cfg, "p", fakeShardConfigs(2), 0)
	l.connect = connector.connect
	l.run = run
	l.Connect()
	return l
}

func TestRedisLimiterCommandDecision(t *testing.T) {
	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, nil, &calls))
	require.False(t, l.CommandAllowed("42", OperationPublish, "chat"))
	require.Equal(t, []string{"p.rl.{42}.publish.ns.chat"}, calls)

	calls = nil
	l = newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(true, nil, &calls))
	require.True(t, l.CommandAllowed("42", OperationRPC, "slow"))
	require.Equal(t, []string{"p.rl.{42}.rpc.m.slow"}, calls)
}

func TestRedisLimiterNoLimitNoRedisCall(t *testing.T) {
	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, nil, &calls))
	// History has no buckets and default is set, so it is limited; unlimited op needs a config without default.
	require.False(t, l.CommandAllowed("42", OperationHistory, ""))

	noDefault := configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 1)},
	}
	calls = nil
	l = newFakeRedisLimiter(noDefault, fakeRedisRunner(false, nil, &calls))
	require.True(t, l.CommandAllowed("42", OperationHistory, ""))
	require.Empty(t, calls)
}

func TestRedisLimiterAnonymousBypass(t *testing.T) {
	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, nil, &calls))
	require.True(t, l.CommandAllowed("", OperationPublish, ""))
	require.True(t, l.ConnectAllowed(""))
	require.Empty(t, calls)
}

func TestRedisLimiterConnectUsesConnectScope(t *testing.T) {
	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, nil, &calls))
	require.False(t, l.ConnectAllowed("42"))
	require.Equal(t, []string{"p.rl.{42}.connect"}, calls)
}

func TestRedisLimiterConnectFallsBackToDefault(t *testing.T) {
	var calls []string
	cfg := configtypes.RateLimitRedisUserCommand{Enabled: true, Default: testContainer(time.Second, 10)}
	l := newFakeRedisLimiter(cfg, fakeRedisRunner(true, nil, &calls))
	require.True(t, l.ConnectAllowed("42"))
	require.Equal(t, []string{"p.rl.{42}.default"}, calls)
}

func TestRedisLimiterFailOpen(t *testing.T) {
	before := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)

	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, errors.New("redis is down"), &calls))
	require.True(t, l.CommandAllowed("42", OperationPublish, "chat"))
	require.True(t, l.ConnectAllowed("42"))
	require.Len(t, calls, 2)

	after := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	require.Equal(t, float64(2), after-before)
}

func TestRedisLimiterLogsErrorsRateLimited(t *testing.T) {
	var calls []string
	l := newFakeRedisLimiter(redisTestConfig(), fakeRedisRunner(false, errors.New("boom"), &calls))

	var logBuf bytes.Buffer
	savedLogger := log.Logger
	log.Logger = zerolog.New(&logBuf)
	defer func() { log.Logger = savedLogger }()

	l.logError(errors.New("boom"))
	require.Equal(t, 1, strings.Count(logBuf.String(), "\n"))
	// The second error within the interval is not logged.
	l.logError(errors.New("boom"))
	require.Equal(t, 1, strings.Count(logBuf.String(), "\n"))
}

func TestRedisLimiterNeedsKey(t *testing.T) {
	l := NewRedisUserLimiter(redisTestConfig(), "p", fakeShardConfigs(1), 0)
	require.True(t, l.NeedsKey(OperationPublish))
	require.True(t, l.NeedsKey(OperationRPC))
	require.False(t, l.NeedsKey(OperationHistory))
	require.False(t, l.NeedsKey(OperationConnect))
}

func TestRedisLimiterDefaultTimeout(t *testing.T) {
	l := NewRedisUserLimiter(redisTestConfig(), "p", fakeShardConfigs(1), 0)
	require.Equal(t, redisDefaultIOTimeout, l.timeout)
	l = NewRedisUserLimiter(redisTestConfig(), "p", fakeShardConfigs(1), 250*time.Millisecond)
	require.Equal(t, 250*time.Millisecond, l.timeout)
}

// newRetryTestLimiter returns a not yet connected limiter with a connector which fails failFor calls,
// and short retry delays.
func newRetryTestLimiter(failFor int32) (*RedisUserLimiter, *flakyConnector) {
	connector := &flakyConnector{failFor: failFor}
	l := NewRedisUserLimiter(redisTestConfig(), "p", fakeShardConfigs(2), 0)
	l.connect = connector.connect
	l.backoffMin = time.Millisecond
	l.backoffMax = 2 * time.Millisecond
	return l, connector
}

func TestRedisLimiterRunRetriesUntilConnected(t *testing.T) {
	l, connector := newRetryTestLimiter(3)
	l.Connect()
	require.False(t, l.connected())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	require.Eventually(t, l.connected, 5*time.Second, time.Millisecond)
	// One attempt in Connect, three failed retries in Run, then the successful one.
	require.Equal(t, int32(4), connector.calls.Load())

	// Connected limiter does not connect again.
	l.Connect()
	require.Equal(t, int32(4), connector.calls.Load())

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context is done")
	}
	// Shards are closed and forgotten when Run stops.
	require.False(t, l.connected())
}

func TestRedisLimiterRunStopsRetryingWhenContextIsDone(t *testing.T) {
	l, connector := newRetryTestLimiter(1000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	require.Eventually(t, func() bool { return connector.calls.Load() >= 3 }, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context is done")
	}
	require.False(t, l.connected())

	// No more attempts after Run returned.
	calls := connector.calls.Load()
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, calls, connector.calls.Load())
}

func TestRedisLimiterNotConnectedAllowsAndCountsThenEnforces(t *testing.T) {
	connector := &flakyConnector{failFor: 100}
	var calls []string
	l := NewRedisUserLimiter(redisTestConfig(), "p", fakeShardConfigs(2), 0)
	l.connect = connector.connect
	l.run = fakeRedisRunner(false, nil, &calls)
	l.Connect()
	require.False(t, l.connected())

	before := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	require.True(t, l.CommandAllowed("42", OperationPublish, "chat"))
	require.True(t, l.ConnectAllowed("42"))
	after := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	require.Equal(t, float64(2), after-before)
	// Nothing reaches Redis while not connected.
	require.Empty(t, calls)

	// After a successful connect the same limiter enforces its limits.
	connector.failFor = 0
	l.Connect()
	require.True(t, l.connected())
	require.False(t, l.CommandAllowed("42", OperationPublish, "chat"))
	require.Equal(t, []string{"p.rl.{42}.publish.ns.chat"}, calls)
}

func TestRedisRetryDelayWithinJitterBounds(t *testing.T) {
	for i := 0; i < 1000; i++ {
		delay := retryDelay(time.Second)
		require.GreaterOrEqual(t, delay, 500*time.Millisecond)
		require.LessOrEqual(t, delay, time.Second)
	}
}
