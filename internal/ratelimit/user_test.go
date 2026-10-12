package ratelimit

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		Default: testContainer(interval, rate),
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

	require.True(t, limiter.CommandAllowed(first, time.Time{}, OperationPublish, ""))
	require.True(t, limiter.CommandAllowed(second, time.Time{}, OperationPublish, ""))
	// The user budget of 2 is spent, both connections are rejected.
	require.False(t, limiter.CommandAllowed(first, time.Time{}, OperationPublish, ""))
	require.False(t, limiter.CommandAllowed(second, time.Time{}, OperationPublish, ""))

	// Another user has its own budget.
	other := &ConnState{}
	limiter.Attach(other, "other")
	t.Cleanup(func() { limiter.Detach(other) })
	require.True(t, limiter.CommandAllowed(other, time.Time{}, OperationPublish, ""))
}

func TestUserAnonymousConnectionBypasses(t *testing.T) {
	limiter := NewUserLimiter(userConfig(time.Hour, 1))
	st := &ConnState{}
	for i := 0; i < 10; i++ {
		require.True(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
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
	require.True(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
	require.False(t, limiter.CommandAllowed(st, time.Time{}, OperationPublish, ""))
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
	require.Equal(t, 0, limiter.store.sweep(current))
	require.Equal(t, 1, storeSize(limiter.store))

	// Released by the last connection at current, idle time is counted from there.
	limiter.Detach(second)
	require.Equal(t, 0, limiter.store.sweep(current.Add(59*time.Second)))
	require.Equal(t, 1, storeSize(limiter.store))

	require.Equal(t, 1, limiter.store.sweep(current.Add(61*time.Second)))
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
	require.Equal(t, 1, limiter.store.sweep(current))

	again := &ConnState{}
	limiter.Attach(again, "user")
	t.Cleanup(func() { limiter.Detach(again) })
	require.Equal(t, 1, storeSize(limiter.store))
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
				if limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	// Check-and-commit under the user lock: exactly the budget is allowed, never more.
	require.Equal(t, int64(budget), allowed.Load())
}
