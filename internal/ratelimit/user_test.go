package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	_ = metrics.Init(metrics.Config{
		Registerer: prometheus.NewRegistry(),
	})
	os.Exit(m.Run())
}

// userConfig returns user command limits with the given default bucket.
func userConfig(interval time.Duration, rate int) configtypes.RateLimitUserCommand {
	return configtypes.RateLimitUserCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Default: testContainer(interval, rate),
		},
	}
}

// storeSize returns the number of user states kept by the store.
func storeSize(s *userStore) int {
	size := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		size += len(s.shards[i].users)
		s.shards[i].mu.Unlock()
	}
	return size
}

func TestUserBucketsSharedBetweenConnections(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Hour, 2))
	first := &ConnState{}
	second := &ConnState{}
	limiter.Attach(first, "user")
	limiter.Attach(second, "user")
	t.Cleanup(func() {
		limiter.Detach(first)
		limiter.Detach(second)
	})

	require.Zero(t, limiter.CommandAllowed(first, time.Time{}, OperationPublish, ""))
	require.Zero(t, limiter.CommandAllowed(second, time.Time{}, OperationPublish, ""))
	// The user budget of 2 is spent, both connections are rejected.
	require.NotZero(t, limiter.CommandAllowed(first, time.Time{}, OperationPublish, ""))
	require.NotZero(t, limiter.CommandAllowed(second, time.Time{}, OperationPublish, ""))

	// Another user has its own budget.
	other := &ConnState{}
	limiter.Attach(other, "other")
	t.Cleanup(func() { limiter.Detach(other) })
	require.Zero(t, limiter.CommandAllowed(other, time.Time{}, OperationPublish, ""))
}

func TestUserAnonymousConnectionBypasses(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Hour, 1))
	st := &ConnState{}
	for i := 0; i < 10; i++ {
		require.Zero(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
	}
	// Detach of a connection which was never attached is a no-op.
	limiter.Detach(st)
	require.Equal(t, 0, storeSize(limiter.store))
}

func TestUserConnectLimit(t *testing.T) {
	limiter := NewUserLimiter(configtypes.RateLimitUserCommand{
		Enabled: true,
		Total:   testContainer(time.Hour, 1),
		Connect: testContainer(time.Hour, 3),
	})
	// Connect is checked against its own buckets only, it does not consume total.
	require.True(t, limiter.ConnectAllowed("user", time.Time{}))
	require.True(t, limiter.ConnectAllowed("user", time.Time{}))
	require.True(t, limiter.ConnectAllowed("user", time.Time{}))
	require.False(t, limiter.ConnectAllowed("user", time.Time{}))

	st := &ConnState{}
	limiter.Attach(st, "user")
	t.Cleanup(func() { limiter.Detach(st) })
	// total was not consumed by connects, so the first command passes it.
	require.Zero(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
	require.NotZero(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
}

func TestUserConnectFallsBackToDefault(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Hour, 1))
	require.True(t, limiter.ConnectAllowed("user", time.Time{}))
	require.False(t, limiter.ConnectAllowed("user", time.Time{}))
}

func TestUserIdleTimeout(t *testing.T) {
	require.Equal(t, 60*time.Second, userIdleTimeout(time.Second))
	require.Equal(t, 90*time.Second, userIdleTimeout(45*time.Second))
	require.Equal(t, 10*time.Minute, userIdleTimeout(5*time.Minute))
}

func TestUserEvictionRefcountAndIdle(t *testing.T) {
	base := time.Now()
	current := base
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return current }

	entriesBefore := testutil.ToFloat64(metrics.RateLimitUserEntries)
	first := &ConnState{}
	second := &ConnState{}
	limiter.Attach(first, "user")
	limiter.Attach(second, "user")
	require.Equal(t, 1, storeSize(limiter.store))
	require.Equal(t, entriesBefore+1, testutil.ToFloat64(metrics.RateLimitUserEntries))

	// Not evicted while a connection still references it, however long it is idle.
	limiter.Detach(first)
	current = base.Add(time.Hour)
	require.Equal(t, 0, sweepAt(limiter.store, current))
	require.Equal(t, 1, storeSize(limiter.store))

	// Released by the last connection at current, idle time is counted from there.
	limiter.Detach(second)
	require.Equal(t, 0, sweepAt(limiter.store, current.Add(59*time.Second)))
	require.Equal(t, 1, storeSize(limiter.store))

	require.Equal(t, 1, sweepAt(limiter.store, current.Add(61*time.Second)))
	require.Equal(t, 0, storeSize(limiter.store))
	require.Equal(t, entriesBefore, testutil.ToFloat64(metrics.RateLimitUserEntries))
}

func TestUserReattachAfterEvictionCreatesNewState(t *testing.T) {
	base := time.Now()
	current := base
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return current }

	st := &ConnState{}
	limiter.Attach(st, "user")
	limiter.Detach(st)
	current = base.Add(2 * time.Minute)
	require.Equal(t, 1, sweepAt(limiter.store, current))

	again := &ConnState{}
	limiter.Attach(again, "user")
	t.Cleanup(func() { limiter.Detach(again) })
	require.Equal(t, 1, storeSize(limiter.store))
}

// sweepAt evicts the states which are due at now, as the eviction timer does, and returns the number evicted.
func sweepAt(s *userStore, now time.Time) int {
	evicted, _, _ := s.evict(nowNano(now))
	return evicted
}

// idleCount returns the number of states in the idle lists. It changes nothing.
func idleCount(s *userStore) int {
	count := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		count += s.shards[i].idleList.Len()
		s.shards[i].mu.Unlock()
	}
	return count
}

// isIdle reports whether the state of userID is in an idle list. It changes nothing.
func isIdle(s *userStore, userID string) bool {
	sh := &s.shards[userShardIndex(userID)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	us, ok := sh.users[userID]
	return ok && us.idleListed
}

// startRun runs the limiter until the returned stop function is called, which requires context.Canceled.
func startRun(t *testing.T, limiter *UserLimiter) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- limiter.Run(ctx)
	}()
	synctest.Wait()
	return func() {
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		synctest.Wait()
	}
}

func TestUserStateSize(t *testing.T) {
	require.Equal(t, uintptr(80), unsafe.Sizeof(UserState{}))
}

func TestUserIdleListHoldsOnlyUnreferencedStates(t *testing.T) {
	base := time.Now()
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return base }
	a1 := &ConnState{}
	a2 := &ConnState{}
	b1 := &ConnState{}
	limiter.Attach(a1, "a")
	limiter.Attach(a2, "a")
	limiter.Attach(b1, "b")

	limiter.Detach(a1)
	require.Equal(t, 0, idleCount(limiter.store))
	limiter.Detach(b1)
	require.True(t, isIdle(limiter.store, "b"))
	require.Equal(t, 1, idleCount(limiter.store))
	limiter.Detach(a2)
	require.Equal(t, 2, idleCount(limiter.store))

	again := &ConnState{}
	limiter.Attach(again, "a")
	t.Cleanup(func() { limiter.Detach(again) })
	require.False(t, isIdle(limiter.store, "a"))
	require.Equal(t, 1, idleCount(limiter.store))
}

func TestUserEvictionFollowsLastRelease(t *testing.T) {
	base := time.Now()
	current := base
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return current }
	for _, release := range []struct {
		userID string
		at     time.Duration
	}{
		{userID: "a", at: 0},
		{userID: "b", at: 10 * time.Second},
		{userID: "c", at: 20 * time.Second},
	} {
		current = base.Add(release.at)
		st := &ConnState{}
		limiter.Attach(st, release.userID)
		limiter.Detach(st)
	}

	require.Equal(t, 2, sweepAt(limiter.store, base.Add(76*time.Second)))
	require.Equal(t, 1, storeSize(limiter.store))
	require.True(t, isIdle(limiter.store, "c"))
}

func TestUserReacquireMovesStateToBack(t *testing.T) {
	base := time.Now()
	current := base
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return current }
	st := &ConnState{}
	limiter.Attach(st, "a")
	limiter.Detach(st)

	current = base.Add(5 * time.Second)
	limiter.Attach(st, "a")
	current = base.Add(50 * time.Second)
	limiter.Detach(st)

	require.Equal(t, 0, sweepAt(limiter.store, base.Add(70*time.Second)))
	require.Equal(t, 1, sweepAt(limiter.store, base.Add(111*time.Second)))
}

func TestUserLinkIdleSkipsEvictedState(t *testing.T) {
	base := time.Now()
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return base }
	store := limiter.store

	us := store.acquire("u")
	us.refs.Add(-1)
	again := store.acquire("u")
	require.Same(t, us, again)
	store.release(again)
	require.Equal(t, 1, sweepAt(store, base.Add(2*userIdleTimeout(time.Second))))

	_, linked := store.linkIdle(us)
	require.False(t, linked)
	require.Equal(t, 0, idleCount(store))

	fresh := store.acquire("u")
	require.NotSame(t, us, fresh)
	require.Equal(t, 1, storeSize(store))
	store.release(fresh)
}

func TestUserEvictBatchesPerShard(t *testing.T) {
	base := time.Now()
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	limiter.store.now = func() time.Time { return base }
	store := limiter.store

	target := userShardIndex("u-0")
	var userIDs []string
	for i := 0; len(userIDs) < userEvictBatch+76; i++ {
		userID := fmt.Sprintf("u-%d", i)
		if userShardIndex(userID) == target {
			userIDs = append(userIDs, userID)
		}
	}
	for _, userID := range userIDs {
		st := &ConnState{}
		limiter.Attach(st, userID)
		limiter.Detach(st)
	}

	require.Equal(t, len(userIDs), sweepAt(store, base.Add(userIdleTimeout(time.Second)+time.Nanosecond)))
	require.Equal(t, 0, storeSize(store))
	require.Equal(t, 0, idleCount(store))
}

func TestUserRunEvictsAtIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := NewUserLimiter(userConfig(time.Second, 10))
		entriesBefore := testutil.ToFloat64(metrics.RateLimitUserEntries)
		stop := startRun(t, limiter)

		st := &ConnState{}
		limiter.Attach(st, "u")
		limiter.Detach(st)

		time.Sleep(60 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, storeSize(limiter.store))

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 0, storeSize(limiter.store))
		require.Equal(t, entriesBefore, testutil.ToFloat64(metrics.RateLimitUserEntries))
		require.Equal(t, int64(0), limiter.store.evictAt.Load())
		stop()
	})
}

func TestUserEvictionRespectsGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := NewUserLimiter(userConfig(time.Second, 10))
		stop := startRun(t, limiter)

		a := &ConnState{}
		limiter.Attach(a, "a")
		limiter.Detach(a)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		b := &ConnState{}
		limiter.Attach(b, "b")
		limiter.Detach(b)

		time.Sleep(50*time.Second + time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 1, storeSize(limiter.store))

		time.Sleep(30*time.Second - time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 1, storeSize(limiter.store))

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 0, storeSize(limiter.store))
		stop()
	})
}

func TestUserRunEvictsStatesIdleBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := NewUserLimiter(userConfig(time.Second, 10))
		st := &ConnState{}
		limiter.Attach(st, "u")
		limiter.Detach(st)

		time.Sleep(10 * time.Second)
		synctest.Wait()
		stop := startRun(t, limiter)

		time.Sleep(50 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, storeSize(limiter.store))

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 0, storeSize(limiter.store))
		stop()
	})
}

func TestUserReacquiredStateCostsOneEmptyRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := NewUserLimiter(userConfig(time.Second, 10))
		stop := startRun(t, limiter)

		st := &ConnState{}
		limiter.Attach(st, "u")
		limiter.Detach(st)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		limiter.Attach(st, "u")

		time.Sleep(55*time.Second + time.Nanosecond)
		synctest.Wait()
		require.Equal(t, 1, storeSize(limiter.store))
		require.Equal(t, int64(0), limiter.store.evictAt.Load())

		limiter.Detach(st)
		require.Equal(t, nowNano(time.Now())+int64(60*time.Second)+1, limiter.store.evictAt.Load())
		stop()
	})
}

func TestUserReleaseLinksIdleWithoutAllocation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := NewUserLimiter(userConfig(time.Second, 10))
		limiter.store.startEviction()
		warm := limiter.store.acquire("u")
		limiter.store.release(warm)
		synctest.Wait()

		allocs := testing.AllocsPerRun(1000, func() {
			us := limiter.store.acquire("u")
			limiter.store.release(us)
		})
		require.Zero(t, allocs)
		limiter.store.stopEviction()
	})
}

func TestUserConcurrentAttachDetachEvict(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	entriesBefore := testutil.ToFloat64(metrics.RateLimitUserEntries)
	store := limiter.store

	done := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			sweepAt(store, time.Now().Add(24*time.Hour))
		}
	}()

	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range 2000 {
				userID := fmt.Sprintf("user-%d", (worker+i)%100)
				st := &ConnState{}
				limiter.Attach(st, userID)
				sh := &store.shards[userShardIndex(userID)]
				sh.mu.Lock()
				mapped := sh.users[userID] == st.user.Load()
				sh.mu.Unlock()
				if !mapped {
					t.Errorf("attached state of %s is not in the store", userID)
				}
				limiter.Detach(st)
			}
		}()
	}
	workers.Wait()
	close(done)
	sweeper.Wait()

	sweepAt(store, time.Now().Add(24*time.Hour))
	require.Equal(t, 0, storeSize(store))
	require.Equal(t, entriesBefore, testutil.ToFloat64(metrics.RateLimitUserEntries))
	for i := range store.shards {
		sh := &store.shards[i]
		sh.mu.Lock()
		listed := 0
		for _, us := range sh.users {
			if us.idleListed {
				listed++
			}
		}
		require.Equal(t, listed, sh.idleList.Len())
		sh.mu.Unlock()
	}
}

// TestUserReattachReleasesPreviousState checks that attaching a connection again releases the state of the first
// attach: the first user is left with no reference, and the second holds exactly one.
func TestUserReattachReleasesPreviousState(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	st := &ConnState{}
	limiter.Attach(st, "first")
	limiter.Attach(st, "second")

	first := limiter.store.acquire("first")
	require.Equal(t, int32(1), first.refs.Load())
	limiter.store.release(first)

	second := limiter.store.acquire("second")
	require.Equal(t, int32(2), second.refs.Load())
	limiter.store.release(second)

	limiter.Detach(st)
	second = limiter.store.acquire("second")
	require.Equal(t, int32(1), second.refs.Load())
	limiter.store.release(second)
}

func TestUserDetachIsIdempotent(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	st := &ConnState{}
	limiter.Attach(st, "user")
	limiter.Detach(st)
	limiter.Detach(st)

	us := limiter.store.acquire("user")
	require.Equal(t, int32(1), us.refs.Load())
	limiter.store.release(us)
}

func TestUserConcurrentChecksOfOneUser(t *testing.T) {
	const (
		goroutines = 16
		perWorker  = 200
		budget     = 1000
	)
	// Interval is long enough that nothing refills during the test.
	limiter := NewUserLimiter(userConfig(time.Hour, budget))

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := &ConnState{}
			limiter.Attach(st, "same")
			defer limiter.Detach(st)
			for i := 0; i < perWorker; i++ {
				if limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") == 0 {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	// Check-and-commit under the user lock: exactly the budget is allowed, never more.
	require.Equal(t, int64(budget), allowed.Load())
}
