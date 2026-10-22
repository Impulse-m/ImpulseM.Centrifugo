package ratelimit

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/intrusive"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

const (
	// userShardCount is the number of independent shards of the user store. Must be a power of two.
	userShardCount = 256
	// userSweepInterval is the shortest time between two eviction runs of idle user states.
	userSweepInterval = 30 * time.Second
	// userMinIdle is the minimum time a user state is kept after its last reference is released.
	userMinIdle = 60 * time.Second
	// userEvictBatch is the number of evictions per hold of a shard lock.
	userEvictBatch = 1024
)

// UserState is a rate limit state shared by all connections of one user. Connections of one user
// may check commands concurrently, so slots are guarded by mu. refs counts attached connections and
// connect checks in progress: a state is evicted only when refs is zero and it has been idle long enough.
// The shard lock guards idleListed, evicted and lastSeen.
type UserState struct {
	slots      []int64
	mu         sync.Mutex
	refs       atomic.Int32
	idleListed bool
	evicted    bool
	lastSeen   int64
	userID     string
	idleLinks  intrusive.Links[UserState]
}

// userShard is a part of the user store guarded by its own mutex.
type userShard struct {
	mu    sync.Mutex
	users map[string]*UserState
	// idleList holds states without references, oldest release first.
	idleList intrusive.List[UserState]
}

// userStore keeps user states in shards. Acquire and eviction take the shard lock. Release takes it only when it
// drops the last reference, to put the state in the idle list.
type userStore struct {
	shards   [userShardCount]userShard
	numSlots int
	idle     int64
	// now is the clock of the store. NewUserLimiter passes time.Now. Tests replace it to place the eviction at exact
	// offsets, which sweepAt then reads; no other code path replaces it.
	now func() time.Time

	// evictAt is the time the eviction timer is armed for, in nowNano, 0 while not armed. It is stored under evictMu
	// and loaded without it.
	evictAt atomic.Int64
	// evictMu guards the fields below and every store to evictAt.
	evictMu sync.Mutex
	// evictOn is set while the limiter runs.
	evictOn bool
	// evictLast is the start of the previous run, in nowNano, 0 before the first.
	evictLast int64
	// evictTimer is created by the first arm.
	evictTimer *time.Timer
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
		store: newUserStore(plan.numSlots, userIdleTimeout(plan.maxInterval, plan.tokenOnly), time.Now),
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
// attached user are not limited. It returns 0 when the command is allowed. Otherwise it returns the wait
// until the command is allowed, and nothing is consumed.
func (l *UserLimiter) CommandAllowed(st *ConnState, at time.Time, op Operation, key string) time.Duration {
	us := st.user.Load()
	if us == nil {
		return 0
	}
	us.mu.Lock()
	wait := l.plan.allow(us.slots, nowNano(at), op, key)
	us.mu.Unlock()
	return time.Duration(wait)
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

// Run evicts idle user states with one timer armed for the earliest one, at most once per userSweepInterval, until the
// context is done. No timer is armed while no state is idle.
func (l *UserLimiter) Run(ctx context.Context) error {
	l.store.startEviction()
	<-ctx.Done()
	l.store.stopEviction()
	return ctx.Err()
}

// userIdleTimeout returns how long a state without references is kept: twice the longest bucket
// interval, or one interval when the plan has token buckets only, but at least userMinIdle. A token
// bucket TAT is at most one interval ahead of the last operation, because newLimit in token_bucket.go
// sets the tolerance to emission times rate, which is at most the interval. After that much idle time
// a state equals a fresh one for every algorithm: a token bucket TAT is in the past, and a window state
// is two windows old, see windowAt.
func userIdleTimeout(maxInterval time.Duration, tokenOnly bool) time.Duration {
	if tokenOnly {
		// A token bucket TAT is at most one interval ahead of the last operation, so one interval of idleness suffices.
		return max(maxInterval, userMinIdle)
	}
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
		s.shards[i].idleList = intrusive.NewList(idleLinks)
	}
	return s
}

// idleLinks returns the links of a state in the idle list of its shard.
func idleLinks(us *UserState) *intrusive.Links[UserState] {
	return &us.idleLinks
}

// acquire returns the state of userID, creating it when missing, and takes a reference to it. An idle state is taken
// out of the idle list.
func (s *userStore) acquire(userID string) *UserState {
	sh := &s.shards[userShardIndex(userID)]
	sh.mu.Lock()
	us, ok := sh.users[userID]
	if !ok {
		us = &UserState{slots: make([]int64, s.numSlots), userID: userID}
		sh.users[userID] = us
		metrics.RateLimitUserEntries.Inc()
	} else if us.idleListed {
		sh.idleList.Remove(us)
		us.idleListed = false
	}
	us.refs.Add(1)
	sh.mu.Unlock()
	return us
}

// release drops a reference to the state. The last reference puts the state at the back of its shard's idle list and
// arms the eviction timer for it.
func (s *userStore) release(us *UserState) {
	if us.refs.Add(-1) != 0 {
		return
	}
	if lastSeen, linked := s.linkIdle(us); linked {
		s.armEviction(lastSeen + s.idle + 1)
	}
}

// linkIdle puts a state without references at the back of the idle list of its shard, with the time it was released
// now. It returns that time, and false when the state was referenced again or evicted meanwhile.
func (s *userStore) linkIdle(us *UserState) (int64, bool) {
	sh := &s.shards[userShardIndex(us.userID)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if us.refs.Load() != 0 || us.evicted {
		return 0, false
	}
	us.lastSeen = nowNano(s.now())
	if us.idleListed {
		sh.idleList.Remove(us)
	}
	sh.idleList.PushBack(us)
	us.idleListed = true
	return us.lastSeen, true
}

// evict removes the states idle for longer than the idle timeout at now (nowNano), at most userEvictBatch per hold of a
// shard lock. It returns their number and when the earliest remaining idle state is due, ok false when none is idle.
func (s *userStore) evict(now int64) (evicted int, next int64, ok bool) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		held := 0
		for head := sh.idleList.Front(); head != nil; head = sh.idleList.Front() {
			if now-head.lastSeen <= s.idle {
				if due := head.lastSeen + s.idle + 1; !ok || due < next {
					next, ok = due, true
				}
				break
			}
			if held == userEvictBatch {
				sh.mu.Unlock()
				sh.mu.Lock()
				held = 0
				continue
			}
			sh.idleList.Remove(head)
			head.idleListed = false
			head.evicted = true
			delete(sh.users, head.userID)
			evicted++
			held++
		}
		sh.mu.Unlock()
	}
	metrics.RateLimitUserEntries.Sub(float64(evicted))
	return evicted, next, ok
}

// armEviction arms the eviction timer, while it is on, for at (nowNano), or userSweepInterval after the start of the
// previous run if that is later. A timer armed for an earlier time stays; seeing that is one atomic load, so a release
// to zero takes no shared lock while the timer is armed.
func (s *userStore) armEviction(at int64) {
	if armed := s.evictAt.Load(); armed != 0 && armed <= at {
		return
	}
	s.evictMu.Lock()
	defer s.evictMu.Unlock()
	if !s.evictOn {
		return
	}
	if s.evictLast != 0 {
		at = max(at, s.evictLast+int64(userSweepInterval))
	}
	if armed := s.evictAt.Load(); armed != 0 && armed <= at {
		return
	}
	s.evictAt.Store(at)
	delay := time.Duration(at - nowNano(s.now()))
	if s.evictTimer == nil {
		s.evictTimer = time.AfterFunc(delay, s.runEviction)
		return
	}
	s.evictTimer.Reset(delay)
}

// runEviction is the callback of the eviction timer: it evicts the due states and arms for the earliest remaining one.
func (s *userStore) runEviction() {
	now := nowNano(s.now())
	s.evictMu.Lock()
	s.evictAt.Store(0)
	s.evictLast = now
	s.evictMu.Unlock()
	if _, next, ok := s.evict(now); ok {
		s.armEviction(next)
	}
}

// startEviction turns the eviction on. Its first run evicts the states idle before the start and arms for the earliest
// remaining one.
func (s *userStore) startEviction() {
	s.evictMu.Lock()
	s.evictOn = true
	s.evictMu.Unlock()
	s.runEviction()
}

// stopEviction turns the eviction off and stops its timer. A run which has started completes and arms nothing.
func (s *userStore) stopEviction() {
	s.evictMu.Lock()
	defer s.evictMu.Unlock()
	s.evictOn = false
	s.evictAt.Store(0)
	if s.evictTimer != nil {
		s.evictTimer.Stop()
	}
}

// userShardSeed seeds userShardIndex. The shards are local to the process, so the seed may be random.
var userShardSeed = maphash.MakeSeed()

// userShardIndex returns the shard of userID. It does not allocate.
func userShardIndex(userID string) int {
	return shardIndex(userShardSeed, userID, userShardCount)
}

// shardIndex returns the shard of key among count shards, count being a power of two. It does not allocate.
func shardIndex(seed maphash.Seed, key string, count int) int {
	return int(maphash.String(seed, key) & uint64(count-1))
}
