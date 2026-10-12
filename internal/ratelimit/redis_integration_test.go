//go:build integration

package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// integrationRedisAddress is the Redis of integration tests, see docker-compose.yml.
const integrationRedisAddress = "localhost:6379"

// uniquePrefix returns a key prefix which no other test run uses, so tests do not share state.
func uniquePrefix() string {
	return fmt.Sprintf("rltest%d", time.Now().UnixNano())
}

// newIntegrationLimiter creates a limiter with its own Redis shards, as a separate node would have.
func newIntegrationLimiter(tb testing.TB, cfg configtypes.RateLimitRedisUserCommand, prefix string) *RedisUserLimiter {
	tb.Helper()
	shards, err := redisshard.BuildRedisShards(configtypes.Redis{Address: []string{integrationRedisAddress}})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		for _, shard := range shards {
			shard.Close()
		}
	})
	return NewRedisUserLimiter(cfg, prefix, shards, 0)
}

// redisIntegrationConfig returns redis_user_command limits of publish with one bucket.
func redisIntegrationConfig(interval time.Duration, rate int) configtypes.RateLimitRedisUserCommand {
	return configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(interval, rate)},
	}
}

// runOnShard runs a Redis command on the first shard of the limiter.
func runOnShard(l *RedisUserLimiter, build func(client rueidis.Client) rueidis.Completed) rueidis.RedisResult {
	return l.shards[0].RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(context.Background(), build(client))
	})
}

func TestRedisLimiterSharedBetweenNodes(t *testing.T) {
	prefix := uniquePrefix()
	cfg := redisIntegrationConfig(time.Minute, 2)
	nodeA := newIntegrationLimiter(t, cfg, prefix)
	nodeB := newIntegrationLimiter(t, cfg, prefix)

	require.True(t, nodeA.CommandAllowed("42", OperationPublish, ""))
	require.True(t, nodeB.CommandAllowed("42", OperationPublish, ""))
	// The budget of 2 is shared by both nodes.
	require.False(t, nodeA.CommandAllowed("42", OperationPublish, ""))
	require.False(t, nodeB.CommandAllowed("42", OperationPublish, ""))

	// Another user has its own budget.
	require.True(t, nodeB.CommandAllowed("43", OperationPublish, ""))
}

func TestRedisLimiterBurstThenRefill(t *testing.T) {
	prefix := uniquePrefix()
	l := newIntegrationLimiter(t, redisIntegrationConfig(time.Second, 2), prefix)

	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.False(t, l.CommandAllowed("42", OperationPublish, ""))

	// One operation is refilled every 500ms (interval 1s, rate 2).
	time.Sleep(600 * time.Millisecond)
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.False(t, l.CommandAllowed("42", OperationPublish, ""))
}

func TestRedisLimiterMultiBucketAllOrNothing(t *testing.T) {
	prefix := uniquePrefix()
	cfg := configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			// The first bucket allows 10 operations, the second only 1.
			Buckets: configtypes.RateLimitBucketList{
				{Interval: configtypes.Duration(time.Minute), Rate: 10},
				{Interval: configtypes.Duration(time.Minute), Rate: 1},
			},
		},
	}
	l := newIntegrationLimiter(t, cfg, prefix)
	key := redisKey(prefix, "42", "publish")

	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	before := runOnShard(l, func(c rueidis.Client) rueidis.Completed {
		return c.B().Hgetall().Key(key).Build()
	})
	beforeFields, err := before.AsStrMap()
	require.NoError(t, err)
	require.Len(t, beforeFields, 2)

	// The second bucket rejects, so the first bucket must not be consumed either.
	require.False(t, l.CommandAllowed("42", OperationPublish, ""))
	require.False(t, l.CommandAllowed("42", OperationPublish, ""))
	after := runOnShard(l, func(c rueidis.Client) rueidis.Completed {
		return c.B().Hgetall().Key(key).Build()
	})
	afterFields, err := after.AsStrMap()
	require.NoError(t, err)
	require.Equal(t, beforeFields, afterFields)
}

func TestRedisLimiterKeyTTLIsSet(t *testing.T) {
	prefix := uniquePrefix()
	l := newIntegrationLimiter(t, redisIntegrationConfig(time.Minute, 5), prefix)
	key := redisKey(prefix, "42", "publish")

	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	pttl, err := runOnShard(l, func(c rueidis.Client) rueidis.Completed {
		return c.B().Pttl().Key(key).Build()
	}).AsInt64()
	require.NoError(t, err)
	require.Greater(t, pttl, int64(0))
	require.LessOrEqual(t, pttl, int64(time.Minute/time.Millisecond))
}

func TestRedisLimiterFailOpenOnClosedShard(t *testing.T) {
	prefix := uniquePrefix()
	cfg := redisIntegrationConfig(time.Minute, 1)
	cfg.Connect = testContainer(time.Minute, 1)
	l := newIntegrationLimiter(t, cfg, prefix)
	before := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)

	// Every call fails on the closed shard, and the commands and the connect are allowed.
	l.shards[0].Close()
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.True(t, l.ConnectAllowed("42"))

	after := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	require.Equal(t, float64(3), after-before)
}

func TestRedisLimiterRunClosesShards(t *testing.T) {
	prefix := uniquePrefix()
	l := newIntegrationLimiter(t, redisIntegrationConfig(time.Minute, 1), prefix)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, l.Run(ctx), context.Canceled)

	// Closed shards fail open.
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
	require.True(t, l.CommandAllowed("42", OperationPublish, ""))
}

// BenchmarkRedisUserCommandCheck64Goroutines checks commands of 64 users from 64 goroutines, one user each.
// The limit is high enough that all commands are allowed, so the benchmark measures Redis round trips.
func BenchmarkRedisUserCommandCheck64Goroutines(b *testing.B) {
	const goroutines = 64
	prefix := uniquePrefix()
	l := newIntegrationLimiter(b, redisIntegrationConfig(time.Minute, 1000000000), prefix)

	perGoroutine := b.N / goroutines
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				l.CommandAllowed(userID, OperationPublish, "")
			}
		}(fmt.Sprint("bench-", g))
	}
	wg.Wait()
}
