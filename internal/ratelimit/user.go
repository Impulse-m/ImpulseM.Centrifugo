package ratelimit

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

const (
	// userShardCount is the number of independent shards of the user store. Must be a power of two.
	userShardCount = 256
	// userSweepInterval is how often idle user states are evicted.
	userSweepInterval = 30 * time.Second
	// userMinIdle is the minimum time a user state is kept after its last reference is released.
	userMinIdle = 60 * time.Second
)

// UserState is a rate limit state shared by all connections of one user. Connections of one user
// may check commands concurrently, so slots are guarded by mu. refs counts attached connections and
// connect checks in progress: a state is evicted only when refs is zero and it has been idle long enough.
type UserState struct {
	slots    []int64
	mu       sync.Mutex
	refs     atomic.Int32
	lastSeen atomic.Int64
}

// userShard is a part of the user store guarded by its own mutex.
type userShard struct {
	mu    sync.Mutex
	users map[string]*UserState
}

// userStore keeps user states in shards. Acquire and sweep take the shard lock, release does not:
// sweep checks refs under the shard lock, and release stores lastSeen before it drops refs.
type userStore struct {
	shards   [userShardCount]userShard
	numSlots int
	idle     int64
	now      func() time.Time
}

// UserLimiter holds compiled user_command limits and the store of user states. Its methods are safe
// for concurrent use, state of a connection is attached once and detached once.
type UserLimiter struct {
	plan  *CommandPlan
	store *userStore
}

// NewUserLimiter compiles user_command limits. Nothing is allocated for connections until they attach.
func NewUserLimiter(cfg configtypes.RateLimitUserCommand) *UserLimiter {
	plan := newUserPlan(cfg)
	return &UserLimiter{
		plan:  plan,
		store: newUserStore(plan.numSlots, userIdleTimeout(plan.maxInterval), time.Now),
	}
}

// Attach resolves the state of userID and attaches it to the connection. A state attached before is released, so a
// connection which attaches again, as a rebind does, holds one reference only. Anonymous connections must not be
// attached.
func (l *UserLimiter) Attach(st *ConnState, userID string) {
	if prev := st.user.Swap(l.store.acquire(userID)); prev != nil {
		l.store.release(prev)
	}
}

// Detach releases the user state attached to the connection. It is called once per connection
// on disconnect, repeated calls do nothing.
func (l *UserLimiter) Detach(st *ConnState) {
	if us := st.user.Swap(nil); us != nil {
		l.store.release(us)
	}
}

// NeedsKey reports whether command checks for op depend on the namespace or method.
func (l *UserLimiter) NeedsKey(op Operation) bool {
	return l.plan.needsKey(op)
}

// CommandAllowed checks and consumes one command of the connection user. Connections without an
// attached user are not limited. It returns false when the command is over the limit, in which case
// nothing is consumed.
func (l *UserLimiter) CommandAllowed(st *ConnState, at time.Time, op Operation, key string) bool {
	us := st.user.Load()
	if us == nil {
		return true
	}
	us.mu.Lock()
	allowed := l.plan.allow(us.slots, nowNano(at), op, key)
	us.mu.Unlock()
	return allowed
}

// ConnectAllowed checks and consumes one connect of userID. The state is referenced only while the
// check runs, so a rejected connect leaves no connection behind.
func (l *UserLimiter) ConnectAllowed(userID string, at time.Time) bool {
	if l.plan.ops[OperationConnect].base == nil {
		// Connect is not limited: skip the store, a reconnect storm costs one shard lock per connect.
		return true
	}
	us := l.store.acquire(userID)
	us.mu.Lock()
	allowed := l.plan.allowConnect(us.slots, nowNano(at))
	us.mu.Unlock()
	l.store.release(us)
	return allowed
}

// Run is the sweeper service of user states. It evicts idle states every userSweepInterval
// until the context is done.
func (l *UserLimiter) Run(ctx context.Context) error {
	ticker := time.NewTicker(userSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			l.store.sweep(l.store.now())
		}
	}
}

// userIdleTimeout returns how long a state without references is kept: twice the longest bucket
// interval, but at least userMinIdle.
func userIdleTimeout(maxInterval time.Duration) time.Duration {
	return max(2*maxInterval, userMinIdle)
}

func newUserStore(numSlots int, idle time.Duration, now func() time.Time) *userStore {
	s := &userStore{
		numSlots: numSlots,
		idle:     int64(idle),
		now:      now,
	}
	for i := range s.shards {
		s.shards[i].users = make(map[string]*UserState)
	}
	return s
}

// acquire returns the state of userID, creating it when missing, and takes a reference to it.
func (s *userStore) acquire(userID string) *UserState {
	sh := &s.shards[userShardIndex(userID)]
	sh.mu.Lock()
	us, ok := sh.users[userID]
	if !ok {
		us = &UserState{slots: make([]int64, s.numSlots)}
		sh.users[userID] = us
		metrics.RateLimitUserEntries.Inc()
	}
	us.refs.Add(1)
	sh.mu.Unlock()
	return us
}

// release drops a reference to the state and marks the state as seen now.
func (s *userStore) release(us *UserState) {
	us.lastSeen.Store(nowNano(s.now()))
	us.refs.Add(-1)
}

// sweep evicts states without references which were idle longer than the idle timeout at now.
// It returns the number of evicted states.
func (s *userStore) sweep(now time.Time) int {
	current := nowNano(now)
	evicted := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for userID, us := range sh.users {
			if us.refs.Load() == 0 && current-us.lastSeen.Load() > s.idle {
				delete(sh.users, userID)
				evicted++
			}
		}
		sh.mu.Unlock()
	}
	metrics.RateLimitUserEntries.Sub(float64(evicted))
	return evicted
}

// userShardSeed seeds userShardIndex. The shards are local to the process, so the seed may be random.
var userShardSeed = maphash.MakeSeed()

// userShardIndex returns the shard of userID. It does not allocate.
func userShardIndex(userID string) int {
	return int(maphash.String(userShardSeed, userID) & (userShardCount - 1))
}
