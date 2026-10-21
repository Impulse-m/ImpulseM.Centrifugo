package ratelimit

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// testWindow is one second in nanoseconds, the window length of most window tests.
const testWindow = int64(time.Second)

// windowIndexFor returns the index of the n-th window of length w counted from the first one after process start.
func windowIndexFor(w int64, n int64) int64 {
	return windowOrigin/w + 1 + n
}

// windowBaseFor returns the nowNano value at the start of the n-th window of length w, see windowIndexFor. The value
// is positive, so token buckets of the same plan see a valid clock.
func windowBaseFor(w int64, n int64) int64 {
	return windowIndexFor(w, n)*w - windowOrigin
}

// takeOrWait takes one operation at now when it is allowed, and returns the wait.
func takeOrWait(algo Algorithm, lim *limit, st []int64, now int64) int64 {
	wait := algo.wait(lim, st, now)
	if wait == 0 {
		algo.take(lim, st, now)
	}
	return wait
}

// panicAlgorithm panics on every call: it proves that a code path does not call an algorithm.
type panicAlgorithm struct {
	Algorithm
}

func TestAlgorithmRegistry(t *testing.T) {
	require.Len(t, algorithms, 3)
	for name, algo := range algorithms {
		require.Equal(t, name, algo.Name())
	}
	require.Equal(t, 1, tokenBucket{}.stateWords())
	require.Equal(t, 3, slidingWindow{}.stateWords())
	require.Equal(t, 2, fixedWindow{}.stateWords())
	require.Equal(t, "token_bucket", algorithmNamed("").Name())
	require.Panics(t, func() { algorithmNamed("leaky") })
}

func TestAlgorithmNamesMatchConfig(t *testing.T) {
	require.Len(t, algorithms, len(configtypes.RateLimitAlgorithmWindows))
	for name, window := range configtypes.RateLimitAlgorithmWindows {
		algo, ok := algorithms[name]
		require.True(t, ok, name)
		_, isToken := algo.(tokenBucket)
		require.Equal(t, window, !isToken, name)
	}
}

func TestAlgorithmResolvedAtCompile(t *testing.T) {
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish:   configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 10), Algorithm: "sliding_window"},
			Subscribe: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 10)},
		},
	})
	publish := p.resolve(OperationPublish, "")
	require.Equal(t, "sliding_window", publish.algo.Name())
	require.False(t, publish.tokenOnly)
	subscribe := p.resolve(OperationSubscribe, "")
	require.Equal(t, "token_bucket", subscribe.algo.Name())
	require.True(t, subscribe.tokenOnly)
	require.Equal(t, 3+1, p.numSlots)
}

func TestTokenOnlyScopeSelected(t *testing.T) {
	token := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   configtypes.RateLimitBuckets{Enabled: true, Buckets: testList(time.Second, 10)},
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 5)},
		},
	})
	require.True(t, token.total.tokenOnly)
	require.True(t, token.resolve(OperationPublish, "").tokenOnly)

	mixed := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 5)},
			History: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 5), Algorithm: "fixed_window"},
		},
	})
	require.True(t, mixed.resolve(OperationPublish, "").tokenOnly)
	require.False(t, mixed.resolve(OperationHistory, "").tokenOnly)
}

func TestTokenOnlyScopeNeverCallsInterface(t *testing.T) {
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   configtypes.RateLimitBuckets{Enabled: true, Buckets: testList(time.Second, 10)},
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 5)},
		},
	})
	p.total.algo = panicAlgorithm{}
	p.resolve(OperationPublish, "").algo = panicAlgorithm{}
	slots := make([]int64, p.numSlots)
	now := nowNano(time.Now())
	require.NotPanics(t, func() {
		for range 20 {
			p.allow(slots, now, OperationPublish, "")
		}
	})
}

func TestTokenBucketWaitExact(t *testing.T) {
	algo := tokenBucket{}
	lim := algo.newLimit(time.Second, 2)
	st := make([]int64, 1)
	now := int64(time.Hour)
	require.Zero(t, takeOrWait(algo, &lim, st, now))
	require.Zero(t, takeOrWait(algo, &lim, st, now))
	require.Equal(t, 500*time.Millisecond, time.Duration(algo.wait(&lim, st, now)))
	require.Equal(t, time.Millisecond, time.Duration(algo.wait(&lim, st, now+int64(499*time.Millisecond))))
	require.Zero(t, algo.wait(&lim, st, now+int64(500*time.Millisecond)))
}

func TestFixedWindowBoundary(t *testing.T) {
	algo := fixedWindow{}
	lim := algo.newLimit(time.Second, 3)
	st := make([]int64, algo.stateWords())
	base := windowBaseFor(testWindow, 100)
	for range 3 {
		require.Zero(t, takeOrWait(algo, &lim, st, base))
	}
	require.Equal(t, time.Second, time.Duration(algo.wait(&lim, st, base)))
	require.Equal(t, time.Duration(1), time.Duration(algo.wait(&lim, st, base+testWindow-1)))
	require.Zero(t, algo.wait(&lim, st, base+testWindow))
	for range 3 {
		require.Zero(t, takeOrWait(algo, &lim, st, base+testWindow))
	}
}

func TestSlidingWindowEstimate(t *testing.T) {
	algo := slidingWindow{}
	lim := algo.newLimit(time.Second, 10)
	st := make([]int64, algo.stateWords())
	base := windowBaseFor(testWindow, 100)
	idx := windowIndexFor(testWindow, 100)
	for range 10 {
		require.Zero(t, takeOrWait(algo, &lim, st, base))
	}
	require.Equal(t, []int64{idx, 10, 0}, st)
	require.Equal(t, time.Second+1, time.Duration(algo.wait(&lim, st, base)))

	// Window 101 at offset 1: the previous window counts 10, the current one 0.
	now := base + testWindow + 1
	require.Zero(t, takeOrWait(algo, &lim, st, now))
	require.Equal(t, []int64{idx + 1, 1, 10}, st)
	require.Equal(t, 100*time.Millisecond, time.Duration(algo.wait(&lim, st, now)))
	require.Equal(t, time.Duration(1), time.Duration(algo.wait(&lim, st, now+100*int64(time.Millisecond)-1)))
	require.Zero(t, algo.wait(&lim, st, now+100*int64(time.Millisecond)))

	// Window 102 at offset 0: the previous window counts 1, nine takes are allowed and the tenth is refused by 1ns.
	at := base + 2*testWindow
	for range 9 {
		require.Zero(t, takeOrWait(algo, &lim, st, at))
	}
	require.Equal(t, time.Duration(1), time.Duration(algo.wait(&lim, st, at)))
}

func TestSlidingWaitOneNanosecond(t *testing.T) {
	algo := slidingWindow{}
	lim := algo.newLimit(time.Second, 10)
	st := make([]int64, algo.stateWords())
	base := windowBaseFor(testWindow, 100)
	for range 10 {
		require.Zero(t, takeOrWait(algo, &lim, st, base))
	}
	at := base + testWindow
	require.Equal(t, time.Duration(1), time.Duration(algo.wait(&lim, st, at)))
	require.Zero(t, algo.wait(&lim, st, at+1))
}

func TestSlidingWindowRollover(t *testing.T) {
	algo := slidingWindow{}
	lim := algo.newLimit(time.Second, 10)
	st := []int64{windowIndexFor(testWindow, 101), 1, 10}
	at := windowBaseFor(testWindow, 105)
	for range 10 {
		require.Zero(t, takeOrWait(algo, &lim, st, at))
	}
	require.Equal(t, windowIndexFor(testWindow, 105), st[0])
}

func TestWindowOutOfOrderNeverRewinds(t *testing.T) {
	later := windowBaseFor(testWindow, 101)
	earlier := windowBaseFor(testWindow, 100) + 500*int64(time.Millisecond)
	for _, algo := range []Algorithm{fixedWindow{}, slidingWindow{}} {
		lim := algo.newLimit(time.Second, 2)
		st := make([]int64, algo.stateWords())
		require.Zero(t, takeOrWait(algo, &lim, st, later))
		idx := windowIndexFor(testWindow, 101)
		require.Zero(t, algo.wait(&lim, st, earlier))
		require.Equal(t, idx, st[0])
		require.Zero(t, takeOrWait(algo, &lim, st, earlier))
		require.Equal(t, idx, st[0])
		wait := algo.wait(&lim, st, earlier)
		require.Positive(t, wait)
		require.Equal(t, idx, st[0])
		if algo.Name() == "fixed_window" {
			require.Equal(t, 1500*time.Millisecond, time.Duration(wait))
		}
	}
}

func TestWindowFarAheadResets(t *testing.T) {
	for _, algo := range []Algorithm{fixedWindow{}, slidingWindow{}} {
		lim := algo.newLimit(time.Second, 2)
		now := windowBaseFor(testWindow, 100)
		st := make([]int64, algo.stateWords())
		st[0] = windowIndexFor(testWindow, 105)
		st[1] = 2
		st[len(st)-1] = 2
		require.Zero(t, algo.wait(&lim, st, now), algo.Name())
		require.Zero(t, takeOrWait(algo, &lim, st, now), algo.Name())
		require.Equal(t, windowIndexFor(testWindow, 100), st[0], algo.Name())
	}
}

func TestWindowIntervalAndRateChangesStartFresh(t *testing.T) {
	publish := func(algorithm string, interval time.Duration, rate int) *scope {
		p := newCommandPlan(configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(interval, rate), Algorithm: algorithm},
			},
		})
		return p.resolve(OperationPublish, "")
	}
	base := publish("sliding_window", time.Second, 10).redisName
	require.Equal(t, "publish~sw.1000.10", base)
	require.NotEqual(t, base, publish("sliding_window", 2*time.Second, 10).redisName)
	require.NotEqual(t, base, publish("sliding_window", time.Second, 11).redisName)
	require.NotEqual(t, base, publish("fixed_window", time.Second, 10).redisName)
	require.Equal(t, "publish", publish("", time.Second, 10).redisName)
}

func TestWaitIsExact(t *testing.T) {
	const rate = 5
	for _, algo := range []Algorithm{fixedWindow{}, slidingWindow{}} {
		rng := rand.New(rand.NewPCG(7, 11))
		lim := algo.newLimit(time.Second, rate)
		st := make([]int64, algo.stateWords())
		now := windowBaseFor(testWindow, 100)
		for range 2000 {
			gap := rng.Int64N(3*testWindow + 1)
			if rng.IntN(4) == 0 {
				gap = -rng.Int64N(testWindow)
			}
			now += gap
			if rng.IntN(50) == 0 {
				// A state with a count above the rate, as a reload with a lower rate can leave.
				idx := floorDiv(windowClock(now), testWindow) + int64(rng.IntN(3)) - 1
				st[0] = idx
				st[1] = rng.Int64N(rate + 3)
				if len(st) == 3 {
					st[2] = rng.Int64N(rate + 3)
				}
			}
			wait := algo.wait(&lim, st, now)
			if wait == 0 {
				algo.take(&lim, st, now)
				continue
			}
			require.Zero(t, algo.wait(&lim, st, now+wait), algo.Name())
			if wait > 1 {
				require.Positive(t, algo.wait(&lim, st, now+wait-1), algo.Name())
			}
		}
	}
}

func TestCommandPlanWaitIsLargestOfScopeAndTotal(t *testing.T) {
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   configtypes.RateLimitBuckets{Enabled: true, Buckets: testList(2*time.Second, 1)},
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 1)},
		},
	})
	slots := make([]int64, p.numSlots)
	now := nowNano(time.Now())
	require.Zero(t, p.allow(slots, now, OperationPublish, ""))
	require.Equal(t, int64(2*time.Second), p.allow(slots, now, OperationPublish, ""))
	require.Zero(t, p.allow(slots, now+int64(2*time.Second), OperationPublish, ""))
}

func TestCommandPlanWaitOfWindowTotal(t *testing.T) {
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   configtypes.RateLimitBuckets{Enabled: true, Algorithm: "sliding_window", Buckets: testList(time.Second, 2)},
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 10)},
		},
	})
	slots := make([]int64, p.numSlots)
	now := windowBaseFor(testWindow, 100)
	require.Zero(t, p.allow(slots, now, OperationPublish, ""))
	require.Zero(t, p.allow(slots, now, OperationPublish, ""))
	require.Equal(t, time.Second+1, time.Duration(p.allow(slots, now, OperationPublish, "")))
}

func TestCommandPlanWaitIsLargestOfBuckets(t *testing.T) {
	for _, algorithm := range []string{"", "sliding_window"} {
		p := newCommandPlan(configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: configtypes.RateLimitChannelBuckets{
					Enabled:   true,
					Algorithm: algorithm,
					Buckets: configtypes.RateLimitBucketList{
						{Interval: configtypes.Duration(time.Second), Rate: 1},
						{Interval: configtypes.Duration(time.Minute), Rate: 1},
					},
				},
			},
		})
		slots := make([]int64, p.numSlots)
		now := windowBaseFor(60*testWindow, 100)
		require.Zero(t, p.allow(slots, now, OperationPublish, ""))
		wait := time.Duration(p.allow(slots, now, OperationPublish, ""))
		if algorithm == "" {
			require.Equal(t, time.Minute, wait)
		} else {
			require.Equal(t, time.Minute+1, wait)
		}
	}
}

func TestWindowCheckNoAllocs(t *testing.T) {
	for _, algorithm := range []string{"fixed_window", "sliding_window"} {
		p := newCommandPlan(configtypes.RateLimitClientCommand{
			Enabled: true,
			Total:   configtypes.RateLimitBuckets{Enabled: true, Algorithm: algorithm, Buckets: testList(time.Second, 10)},
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Algorithm: algorithm, Buckets: testList(time.Second, 10)},
			},
		})
		slots := make([]int64, p.numSlots)
		now := windowBaseFor(testWindow, 100)
		allocs := testing.AllocsPerRun(100, func() {
			p.allow(slots, now, OperationPublish, "")
		})
		require.Zero(t, allocs, algorithm)
	}
}

func TestSlidingStateIsResetAfterIdle(t *testing.T) {
	// After two windows of idle time a state is equivalent to a fresh one, which is what user eviction relies on.
	for _, algo := range []Algorithm{fixedWindow{}, slidingWindow{}} {
		rng := rand.New(rand.NewPCG(3, 5))
		lim := algo.newLimit(time.Second, 4)
		st := make([]int64, algo.stateWords())
		now := windowBaseFor(testWindow, 100)
		for range 200 {
			now += rng.Int64N(testWindow)
			takeOrWait(algo, &lim, st, now)
		}
		later := now + 2*testWindow
		fresh := make([]int64, algo.stateWords())
		for range 20 {
			require.Equal(t, algo.wait(&lim, fresh, later), algo.wait(&lim, st, later), algo.Name())
			takeOrWait(algo, &lim, st, later)
			takeOrWait(algo, &lim, fresh, later)
			later += rng.Int64N(testWindow / 4)
		}
	}
}

func TestUserStateWordsSize(t *testing.T) {
	l := NewUserLimiter(configtypes.RateLimitUserCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish:   configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 5)},
			Subscribe: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Second, 3), Algorithm: "sliding_window"},
		},
	})
	us := l.store.acquire("u")
	require.Len(t, us.slots, l.plan.numSlots)
	require.Equal(t, 1+3, l.plan.numSlots)
	l.store.release(us)
}

func TestRedisScriptsHaveOneClock(t *testing.T) {
	for _, source := range []string{tokenBucketSource, fixedWindowSource, slidingWindowSource} {
		require.Equal(t, 1, strings.Count(source, "redis.call('TIME')"))
		require.Contains(t, source, "local t = redis.call('TIME')\n")
	}
}

func TestRedisArgsLayout(t *testing.T) {
	lims := []limit{windowLimit(time.Second, 10)}
	require.Equal(t, []string{"2000", "1", "1000000", "10"}, slidingWindow{}.redisArgs(lims, time.Second))
	require.Equal(t, []string{"1000", "1", "1000000", "10"}, fixedWindow{}.redisArgs(lims, time.Second))
	require.Equal(t, []string{"1000", "1", "100000", "1000000"}, tokenBucket{}.redisArgs(
		[]limit{tokenBucket{}.newLimit(time.Second, 10)}, time.Second,
	))
}

func TestRedisScopeNameForAlgorithm(t *testing.T) {
	p := newCommandPlan(configtypes.RateLimitClientCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Publish: configtypes.RateLimitChannelBuckets{
				Enabled:   true,
				Algorithm: "fixed_window",
				Buckets:   testList(time.Minute, 100),
				NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
					{NamespaceName: "chat", Enabled: true, Algorithm: "sliding_window", Buckets: testList(time.Second, 3)},
				},
			},
		},
	})
	require.Equal(t, "app.rl.{42}.publish~fw.60000.100", redisKey("app", "42", p.resolve(OperationPublish, "").redisName))
	require.Equal(t, "app.rl.{42}.publish.ns.chat~sw.1000.3", redisKey("app", "42", p.resolve(OperationPublish, "chat").redisName))
}

func TestWaitOfDecodesScriptResults(t *testing.T) {
	wait, err := waitOf(1)
	require.NoError(t, err)
	require.Zero(t, wait)

	wait, err = waitOf(0)
	require.NoError(t, err)
	require.Equal(t, time.Duration(1), wait)

	wait, err = waitOf(-1500000)
	require.NoError(t, err)
	require.Equal(t, 1500*time.Millisecond, wait)

	// A huge wait is clamped, so that it converts to nanoseconds without overflow.
	wait, err = waitOf(-math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, time.Duration(math.MaxInt64/1000)*time.Microsecond, wait)
	require.Positive(t, wait)

	_, err = waitOf(math.MinInt64)
	require.ErrorIs(t, err, errMalformedWait)
	_, err = waitOf(5)
	require.ErrorIs(t, err, errMalformedWait)
}
