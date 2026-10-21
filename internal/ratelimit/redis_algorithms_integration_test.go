//go:build integration

package ratelimit

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// clockScript returns the rate limit script of algo with its clock replaced: the script reads the time from the last
// ARGV entry instead of Redis TIME. The production script is unchanged, the substitution is test-only.
func clockScript(t testing.TB, algo Algorithm) *rueidis.Lua {
	t.Helper()
	const clock = "local t = redis.call('TIME')"
	var source, prefix string
	switch algo.(type) {
	case tokenBucket:
		source = tokenBucketSource
	case fixedWindow:
		source, prefix = fixedWindowSource, luaDivisions
	case slidingWindow:
		source, prefix = slidingWindowSource, luaDivisions
	}
	require.Equal(t, 1, strings.Count(source, clock))
	return rueidis.NewLuaScript(prefix + strings.Replace(source, clock, "local t = {'0', ARGV[#ARGV]}", 1))
}

// runClockScript runs script on the first shard of l for key, with the clock nowUs in microseconds, and returns the wait.
func runClockScript(t testing.TB, l *RedisUserLimiter, script *rueidis.Lua, key string, argv []string, nowUs int64) time.Duration {
	t.Helper()
	args := append(append([]string{}, argv...), strconv.FormatInt(nowUs, 10))
	keys := []string{key}
	res := firstShard(l).RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return script.Exec(context.Background(), client, keys, args)
	})
	v, err := res.AsInt64()
	require.NoError(t, err)
	wait, err := waitOf(v)
	require.NoError(t, err)
	return wait
}

// integrationTime returns the window clock in nanoseconds of window n of length w, plus offset.
func integrationTime(w int64, n int64, offset int64) int64 {
	return windowIndexFor(w, n)*w + offset
}

// integrationAlgorithms returns the algorithms which the Redis tests run for.
func integrationAlgorithms() []Algorithm {
	return []Algorithm{tokenBucket{}, fixedWindow{}, slidingWindow{}}
}

func TestRedisAlgorithmsMatchMemory(t *testing.T) {
	for _, algo := range integrationAlgorithms() {
		lim := algo.newLimit(time.Second, 4)
		lims := []limit{lim}
		l := newIntegrationLimiter(t, redisIntegrationConfig(time.Second, 4), uniquePrefix())
		script := clockScript(t, algo)
		argv := algo.redisArgs(lims, time.Second)
		key := uniquePrefix() + ".rl.{42}.x"
		st := make([]int64, algo.stateWords())
		rng := rand.New(rand.NewPCG(1, 2))
		at := integrationTime(testWindow, 1000, 0)
		for range 300 {
			at += rng.Int64N(3*testWindow/1000) * 1000
			now := at - windowOrigin
			memWait := algo.wait(&lim, st, now)
			redisWait := runClockScript(t, l, script, key, argv, at/1000)
			require.Equal(t, memWait == 0, redisWait == 0, algo.Name())
			if memWait == 0 {
				algo.take(&lim, st, now)
				continue
			}
			require.InDelta(t, float64(memWait), float64(redisWait), 1000, algo.Name())
		}
	}
}

// TestRedisWaitIsMaxOfBuckets checks that a refusal reports the largest wait of the buckets, as memory does. One
// operation takes both buckets, then the next one at the same time waits for the minute bucket.
func TestRedisWaitIsMaxOfBuckets(t *testing.T) {
	for _, algo := range integrationAlgorithms() {
		lims := []limit{algo.newLimit(time.Second, 1), algo.newLimit(time.Minute, 1)}
		l := newIntegrationLimiter(t, redisIntegrationConfig(time.Second, 1), uniquePrefix())
		script := clockScript(t, algo)
		argv := algo.redisArgs(lims, time.Minute)
		key := uniquePrefix() + ".rl.{42}.x"
		at := integrationTime(60*testWindow, 100, 0)
		require.Zero(t, runClockScript(t, l, script, key, argv, at/1000))
		redisWait := runClockScript(t, l, script, key, argv, at/1000)

		now := at - windowOrigin
		var memWait time.Duration
		for i := range lims {
			st := make([]int64, algo.stateWords())
			algo.take(&lims[i], st, now)
			memWait = max(memWait, time.Duration(algo.wait(&lims[i], st, now)))
		}
		require.Positive(t, redisWait)
		require.InDelta(t, float64(memWait), float64(redisWait), 1000, algo.Name())
	}
}

func TestRedisTTLIsSet(t *testing.T) {
	for _, tc := range []struct {
		algo Algorithm
		ttl  time.Duration
	}{
		{algo: fixedWindow{}, ttl: time.Second},
		{algo: slidingWindow{}, ttl: 2 * time.Second},
	} {
		l := newIntegrationLimiter(t, redisIntegrationConfig(time.Second, 10), uniquePrefix())
		script := clockScript(t, tc.algo)
		argv := tc.algo.redisArgs([]limit{tc.algo.newLimit(time.Second, 10)}, time.Second)
		key := uniquePrefix() + ".rl.{42}.x"
		require.Zero(t, runClockScript(t, l, script, key, argv, integrationTime(testWindow, 1000, 0)/1000))
		res := runOnShard(l, func(client rueidis.Client) rueidis.Completed {
			return client.B().Pttl().Key(key).Build()
		})
		ttl, err := res.AsInt64()
		require.NoError(t, err)
		require.InDelta(t, tc.ttl.Milliseconds(), ttl, 1, tc.algo.Name())
	}
}

func TestRedisOutOfOrderNeverRewinds(t *testing.T) {
	for _, algo := range []Algorithm{fixedWindow{}, slidingWindow{}} {
		lim := algo.newLimit(time.Second, 2)
		l := newIntegrationLimiter(t, redisIntegrationConfig(time.Second, 2), uniquePrefix())
		script := clockScript(t, algo)
		argv := algo.redisArgs([]limit{lim}, time.Second)
		key := uniquePrefix() + ".rl.{42}.x"
		later := integrationTime(testWindow, 101, 0)
		earlier := integrationTime(testWindow, 100, 500*int64(time.Millisecond))
		require.Zero(t, runClockScript(t, l, script, key, argv, later/1000))
		require.Zero(t, runClockScript(t, l, script, key, argv, earlier/1000))
		require.Positive(t, runClockScript(t, l, script, key, argv, earlier/1000))
		res := runOnShard(l, func(client rueidis.Client) rueidis.Completed {
			return client.B().Hget().Key(key).Field("0").Build()
		})
		idx, err := res.AsInt64()
		require.NoError(t, err)
		require.Equal(t, windowIndexFor(testWindow, 101), idx, algo.Name())
	}
}

func TestRedisLimiterRealTimeSmoke(t *testing.T) {
	cfg := configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 2), Algorithm: "sliding_window"},
		},
	}
	l := newIntegrationLimiter(t, cfg, uniquePrefix())
	require.Zero(t, l.CommandAllowed("42", OperationPublish, ""))
	require.Zero(t, l.CommandAllowed("42", OperationPublish, ""))
	wait := l.CommandAllowed("42", OperationPublish, "")
	require.Positive(t, wait)
	require.LessOrEqual(t, wait, time.Second)
}
