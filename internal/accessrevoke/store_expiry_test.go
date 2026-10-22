package accessrevoke

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/centrifugal/centrifuge"

	"github.com/stretchr/testify/require"
)

// armedAt returns the time the expiry timer is armed for, zero while it is not armed.
func armedAt(store *Store) time.Time {
	store.expiryMu.Lock()
	defer store.expiryMu.Unlock()
	return store.expiryAt
}

// requireArmedAt checks the time the expiry timer is armed for, zero when it is not armed.
func requireArmedAt(t *testing.T, store *Store, want time.Time) {
	t.Helper()
	got := armedAt(store)
	if want.IsZero() {
		require.True(t, got.IsZero(), "timer armed for %v, want not armed", got)
		return
	}
	require.True(t, got.Equal(want), "timer armed for %v, want %v", got, want)
}

// sessionIDOf returns a session identifier of the index, hashed into a key.
func sessionIDOf(index int) string {
	return fmt.Sprintf("session-%d", index)
}

func TestStoreRewritesKeepOneItemPerKey(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for range 1000 {
		clock++
		require.NoError(t, store.BlockUser("u", 0))
		require.NoError(t, store.RevokeSession(testUUID, 0, 0, nil))
		require.NoError(t, store.InvalidateUser("u", 1, 0, 0, 0))
	}
	require.Len(t, store.blockedExpiry, 1)
	require.Len(t, store.sessionsExpiry, 1)
	require.Len(t, store.usersExpiry, 1)

	clock = 2201
	store.runExpiry()
	require.Equal(t, Sizes{Blocked: 1, Sessions: 1, Users: 1}, store.Sizes())
	require.Len(t, store.blockedExpiry, 1)
	require.Len(t, store.sessionsExpiry, 1)
	require.Len(t, store.usersExpiry, 1)
	require.Equal(t, int64(3200), store.blockedExpiry[0].expireAt)
	require.Equal(t, int64(3200), store.sessionsExpiry[0].expireAt)
	require.Equal(t, int64(3200), store.usersExpiry[0].expireAt)

	clock = 3200
	store.runExpiry()
	require.Equal(t, Sizes{}, store.Sizes())
	require.Empty(t, store.blockedExpiry)
	require.Empty(t, store.sessionsExpiry)
	require.Empty(t, store.usersExpiry)
}

func TestStoreExtendedEntryIsQueuedAgain(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("u", 1100))
	require.NoError(t, store.BlockUser("u", 1300))
	require.Len(t, store.blockedExpiry, 1)

	clock = 1200
	store.runExpiry()
	require.Equal(t, 1, store.Sizes().Blocked)
	require.Equal(t, Blocked, store.Check("u", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, int64(1300), store.blockedExpiry[0].expireAt)

	clock = 1300
	store.runExpiry()
	require.Equal(t, 0, store.Sizes().Blocked)
	require.Empty(t, store.blockedExpiry)
}

func TestStoreUnblockLeavesStaleItem(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("a", 1300))
	require.NoError(t, store.BlockUser("b", 1300))
	require.NoError(t, store.BlockUser("c", 1100))
	require.NoError(t, store.UnblockUser("c"))
	require.Len(t, store.blockedExpiry, 3)

	clock = 1100
	store.runExpiry()
	require.Len(t, store.blockedExpiry, 2)
	require.Equal(t, 2, store.Sizes().Blocked)
	require.Equal(t, int64(1300), store.blockedExpiry[0].expireAt)
}

func TestStoreStaleMajorityIsCompacted(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for i := range 10 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("u%d", i), 1300))
	}
	for i := range 5 {
		require.NoError(t, store.UnblockUser(fmt.Sprintf("u%d", i)))
	}
	require.Len(t, store.blockedExpiry, 10)

	require.NoError(t, store.UnblockUser("u5"))
	store.expireDue(clock)
	require.Len(t, store.blockedExpiry, 4)
	require.Equal(t, 4, cap(store.blockedExpiry))
	require.Equal(t, 4, store.Sizes().Blocked)
	require.Equal(t, Blocked, store.Check("u6", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Equal(t, Allowed, store.Check("u5", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)

	require.NoError(t, store.UnblockUser("u6"))
	require.NoError(t, store.UnblockUser("u7"))
	require.Len(t, store.blockedExpiry, 4)

	require.NoError(t, store.UnblockUser("u8"))
	store.expireDue(clock)
	require.Len(t, store.blockedExpiry, 1)

	require.NoError(t, store.UnblockUser("u9"))
	store.expireDue(clock)
	require.Nil(t, store.blockedExpiry)
	require.Equal(t, 0, store.Sizes().Blocked)
}

func TestStoreExpireContinuesPastOneBatch(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for i := range 300 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("u-%d", i), 1100))
	}
	require.NoError(t, store.BlockUser("late", 2000))

	clock = 1100
	store.runExpiry()
	require.Equal(t, 1, store.Sizes().Blocked)
	require.Len(t, store.blockedExpiry, 1)
}

func TestStoreReblockEarlierExpiresOnTime(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("v", 1500))
	require.NoError(t, store.BlockUser("u", 1300))
	require.NoError(t, store.UnblockUser("u"))
	require.NoError(t, store.BlockUser("u", 1100))
	require.Len(t, store.blockedExpiry, 3)

	clock = 1100
	store.runExpiry()
	require.Equal(t, 1, store.Sizes().Blocked)
	require.Equal(t, Allowed, store.Check("u", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Len(t, store.blockedExpiry, 2)

	clock = 1300
	store.runExpiry()
	require.Len(t, store.blockedExpiry, 1)
}

func TestStoreReblockLaterIsQueuedAgain(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("v", 1500))
	require.NoError(t, store.BlockUser("u", 1100))
	require.NoError(t, store.UnblockUser("u"))
	require.NoError(t, store.BlockUser("u", 1300))

	clock = 1100
	store.runExpiry()
	require.Equal(t, 2, store.Sizes().Blocked)
	require.Equal(t, Blocked, store.Check("u", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.Len(t, store.blockedExpiry, 3)

	clock = 1300
	store.runExpiry()
	require.Equal(t, 1, store.Sizes().Blocked)
	require.Len(t, store.blockedExpiry, 1)
}

func TestStoreExpireVisitsOnlyDueItems(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for i := range 100000 {
		require.NoError(t, store.InvalidateUser(fmt.Sprintf("u-%d", i), 1, 0, 0, 2000))
	}
	require.NoError(t, store.InvalidateUser("early", 1, 0, 0, 1100))

	visits := 0
	kind := store.usersKind()
	kind.expireAtOf = func(entry userEntry) int64 {
		visits++
		return entry.expireAt
	}
	next, ok := expireKind(kind, 1200)
	require.True(t, ok)
	require.Equal(t, int64(2000), next)
	require.Equal(t, 1, visits)
	require.Equal(t, 100000, store.Sizes().Users)
	require.Len(t, store.usersExpiry, 100000)
}

func TestExpiryHeapPopsInOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var order expiryHeap
	for range 10000 {
		order.push(expiryItem{expireAt: rng.Int64N(1 << 40)})
	}
	require.Len(t, order, 10000)

	previous := int64(-1)
	for len(order) > 0 {
		item := order.pop()
		require.GreaterOrEqual(t, item.expireAt, previous)
		previous = item.expireAt
	}
	require.Empty(t, order)
}

func TestExpiryHeapPushPopDoesNotAllocate(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	order := make(expiryHeap, 0, 10001)
	for range 10000 {
		order.push(expiryItem{expireAt: rng.Int64N(1 << 40)})
	}
	item := expiryItem{expireAt: 1 << 39}
	allocs := testing.AllocsPerRun(1000, func() {
		order.push(item)
		order.pop()
	})
	require.Zero(t, allocs)
}

func TestStoreConcurrentExpiryKeepsInvariants(t *testing.T) {
	var clock atomic.Int64
	clock.Store(testNow)
	store := NewStore(20 * time.Minute)
	store.nowUnix = clock.Load

	var writers sync.WaitGroup
	for worker := range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			rng := rand.New(rand.NewPCG(uint64(worker), 7))
			for range 1000 {
				now := clock.Load()
				user := fmt.Sprintf("u-%d", rng.IntN(20))
				expireAt := now - 5 + rng.Int64N(56)
				switch rng.IntN(4) {
				case 0:
					_ = store.BlockUser(user, expireAt)
				case 1:
					_ = store.UnblockUser(user)
				case 2:
					_ = store.RevokeSession(sessionIDOf(rng.IntN(20)), 0, expireAt, nil)
				default:
					_ = store.InvalidateUser(user, 1, 0, 0, expireAt)
				}
			}
		}()
	}

	done := make(chan struct{})
	var expirer sync.WaitGroup
	expirer.Add(1)
	go func() {
		defer expirer.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			clock.Add(1)
			store.runExpiry()
		}
	}()
	writers.Wait()
	close(done)
	expirer.Wait()

	store.blockedMu.Lock()
	require.LessOrEqual(t, len(store.blocked), len(store.blockedExpiry))
	require.LessOrEqual(t, len(store.blockedExpiry), 2*len(store.blocked))
	requireEveryKeyQueued(t, store.blocked, store.blockedExpiry)
	store.blockedMu.Unlock()

	store.sessionsMu.Lock()
	require.Len(t, store.sessionsExpiry, len(store.sessions))
	requireEveryKeyQueued(t, store.sessions, store.sessionsExpiry)
	store.sessionsMu.Unlock()

	store.usersMu.Lock()
	require.Len(t, store.usersExpiry, len(store.users))
	requireEveryKeyQueued(t, store.users, store.usersExpiry)
	store.usersMu.Unlock()

	clock.Store(clock.Load() + 3600)
	store.runExpiry()
	require.Equal(t, Sizes{}, store.Sizes())
	require.Empty(t, store.blockedExpiry)
	require.Empty(t, store.sessionsExpiry)
	require.Empty(t, store.usersExpiry)
}

// requireEveryKeyQueued checks that every key of the map has an item in the order.
func requireEveryKeyQueued[V any](t *testing.T, entries map[[16]byte]V, order expiryHeap) {
	t.Helper()
	queued := make(map[[16]byte]bool, len(order))
	for _, item := range order {
		queued[item.key] = true
	}
	for key := range entries {
		require.True(t, queued[key], "key %x has no item", key)
	}
}

func TestStoreUnblockDuringExpiryKeepsInvariants(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for i := range 100 {
		require.NoError(t, store.BlockUser(fmt.Sprintf("late-%d", i), 1000000))
	}

	for range 20 {
		for i := range 600 {
			require.NoError(t, store.BlockUser(fmt.Sprintf("u-%d", i), clock+1+int64(i)))
		}
		clock += 600

		start := make(chan struct{})
		unblocked := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			for {
				select {
				case <-unblocked:
					store.runExpiry()
					return
				default:
					store.runExpiry()
				}
			}
		}()
		go func() {
			defer workers.Done()
			<-start
			for i := 599; i >= 100; i-- {
				_ = store.UnblockUser(fmt.Sprintf("u-%d", i))
			}
			close(unblocked)
		}()
		close(start)
		workers.Wait()

		store.blockedMu.Lock()
		require.LessOrEqual(t, len(store.blocked), len(store.blockedExpiry))
		require.LessOrEqual(t, len(store.blockedExpiry), 2*len(store.blocked))
		requireEveryKeyQueued(t, store.blocked, store.blockedExpiry)
		store.blockedMu.Unlock()
	}

	require.Equal(t, Sizes{Blocked: 100}, store.Sizes())
	require.Len(t, store.blockedExpiry, 100)

	clock = 1000000
	store.runExpiry()
	require.Equal(t, Sizes{}, store.Sizes())
	require.Empty(t, store.blockedExpiry)
}

func TestStoreExpiryTimerArmsForNewKeysOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewStore(20 * time.Minute)
		t0 := time.Now().Unix()
		start := time.Now()
		store.startExpiry(30 * time.Second)
		synctest.Wait()
		requireArmedAt(t, store, time.Time{})

		require.NoError(t, store.BlockUser("u", t0+100))
		requireArmedAt(t, store, start.Add(100*time.Second))
		require.NoError(t, store.RevokeSession(testUUID, 0, t0+50, nil))
		requireArmedAt(t, store, start.Add(50*time.Second))
		require.NoError(t, store.InvalidateUser("u", 1, 0, 0, t0+200))
		require.NoError(t, store.BlockUser("u", t0+300))
		requireArmedAt(t, store, start.Add(50*time.Second))
		require.Len(t, store.blockedExpiry, 1)

		time.Sleep(50 * time.Second)
		synctest.Wait()
		require.Equal(t, 0, store.Sizes().Sessions)
		requireArmedAt(t, store, start.Add(100*time.Second))

		time.Sleep(50 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, store.Sizes().Blocked)
		requireArmedAt(t, store, start.Add(200*time.Second))

		time.Sleep(100 * time.Second)
		synctest.Wait()
		require.Equal(t, 0, store.Sizes().Users)
		requireArmedAt(t, store, start.Add(300*time.Second))

		time.Sleep(100 * time.Second)
		synctest.Wait()
		require.Equal(t, 0, store.Sizes().Blocked)
		requireArmedAt(t, store, time.Time{})

		store.stopExpiry()
	})
}

func TestStoreExpiryRespectsGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewStore(20 * time.Minute)
		t0 := time.Now().Unix()
		start := time.Now()
		store.startExpiry(30 * time.Second)
		synctest.Wait()
		require.NoError(t, store.BlockUser("u1", t0+100))
		require.NoError(t, store.BlockUser("u2", t0+110))

		time.Sleep(100 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, store.Sizes().Blocked)
		requireArmedAt(t, store, start.Add(130*time.Second))

		time.Sleep(29 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, store.Sizes().Blocked)

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 0, store.Sizes().Blocked)

		store.stopExpiry()
	})
}

func TestStoreExpiryClampsFarExpireAt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewStore(20 * time.Minute)
		t0 := time.Now().Unix()
		start := time.Now()
		store.startExpiry(0)
		synctest.Wait()
		require.NoError(t, store.BlockUser("u", t0+10*365*24*3600))
		requireArmedAt(t, store, start.Add(24*time.Hour))

		time.Sleep(24 * time.Hour)
		synctest.Wait()
		require.Equal(t, 1, store.Sizes().Blocked)
		requireArmedAt(t, store, start.Add(48*time.Hour))

		store.stopExpiry()
	})
}

// BenchmarkStoreExpireDue measures one run of the expiry pass over a store of N sessions: none-due, where nothing is
// due and the time must not depend on N, and burst, where all of them are due.
func BenchmarkStoreExpireDue(b *testing.B) {
	const n int = 1000000
	b.Run("none-due", func(b *testing.B) {
		store := benchSessionStore(b, n, testNow+1000)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			store.expireDue(testNow)
		}
	})
	b.Run("burst", func(b *testing.B) {
		b.ReportAllocs()
		var total time.Duration
		for range b.N {
			b.StopTimer()
			store := benchSessionStore(b, n, testNow)
			b.StartTimer()
			start := time.Now()
			store.expireDue(testNow + 1)
			total += time.Since(start)
			b.StopTimer()
		}
		b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*n), "ns/item")
	})
}

// benchSessionStore returns a store of n sessions which expire at expireAt, inserted into the map and the order.
func benchSessionStore(b *testing.B, n int, expireAt int64) *Store {
	b.Helper()
	clock := testNow
	store := newClockStore(&clock)
	store.sessions = make(map[[16]byte]sessionEntry, n)
	store.sessionsExpiry = make(expiryHeap, 0, n)
	for i := range n {
		var key [16]byte
		binary.BigEndian.PutUint64(key[8:], uint64(i))
		store.sessions[key] = sessionEntry{expireAt: expireAt}
		store.sessionsExpiry = append(store.sessionsExpiry, expiryItem{expireAt: expireAt, key: key})
	}
	heap.Init(&store.sessionsExpiry)
	return store
}

// BenchmarkExpiryHeapGrowth times each push into an empty order of n random items, so the longest push includes the
// growth copy of the order.
func BenchmarkExpiryHeapGrowth(b *testing.B) {
	const n int = 1000000
	b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
		rng := rand.New(rand.NewPCG(5, 6))
		items := make([]expiryItem, n)
		for i := range items {
			items[i] = expiryItem{expireAt: rng.Int64N(1 << 40)}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var order expiryHeap
			var longest time.Duration
			for _, item := range items {
				start := time.Now()
				order.push(item)
				longest = max(longest, time.Since(start))
			}
			b.ReportMetric(float64(longest.Nanoseconds()), "max-ns/push")
		}
	})
}

// BenchmarkStoreUnblockMass times each UnblockUser of n blocked users with random expiries, from the first to the last.
func BenchmarkStoreUnblockMass(b *testing.B) {
	const n int = 1000000
	b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
		rng := rand.New(rand.NewPCG(7, 8))
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			clock := testNow
			store := newClockStore(&clock)
			for i := range n {
				require.NoError(b, store.BlockUser(fmt.Sprintf("u-%d", i), testNow+1+rng.Int64N(1<<20)))
			}
			b.StartTimer()
			var total time.Duration
			var longest time.Duration
			for i := range n {
				start := time.Now()
				require.NoError(b, store.UnblockUser(fmt.Sprintf("u-%d", i)))
				elapsed := time.Since(start)
				total += elapsed
				longest = max(longest, elapsed)
			}
			b.StopTimer()
			b.ReportMetric(float64(total.Nanoseconds())/float64(n), "ns/unblock")
			b.ReportMetric(float64(longest.Nanoseconds()), "max-ns/unblock")
		}
	})
}

// BenchmarkStoreExpireCompaction times the one hold of an expiry pass which removes 256 due entries and then compacts
// the blocked kind: the order is at twice the map before the pass, so the deletes tip the ratio.
func BenchmarkStoreExpireCompaction(b *testing.B) {
	const n int = 1000000
	b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			clock := testNow
			store := newClockStore(&clock)
			store.blocked = make(map[[16]byte]int64, n)
			order := make(expiryHeap, 0, 2*n+1)
			for i := range n {
				var key [16]byte
				binary.BigEndian.PutUint64(key[8:], uint64(i))
				expireAt := int64(2000)
				if i < expireBatch {
					expireAt = 1100
				}
				store.blocked[key] = expireAt
				order = append(order, expiryItem{expireAt: expireAt, key: key})
			}
			for i := range n {
				var key [16]byte
				binary.BigEndian.PutUint64(key[:8], 1)
				binary.BigEndian.PutUint64(key[8:], uint64(i))
				order = append(order, expiryItem{expireAt: 3000, key: key})
			}
			heap.Init(&order)
			store.blockedExpiry = order
			b.StartTimer()
			start := time.Now()
			store.expireDue(1200)
			elapsed := time.Since(start)
			b.StopTimer()
			if len(store.blockedExpiry) != 999744 || len(store.blocked) != 999744 {
				b.Fatalf("order %d and map %d after the pass, want 999744 each", len(store.blockedExpiry), len(store.blocked))
			}
			b.ReportMetric(float64(elapsed.Nanoseconds()), "max-ns/hold")
		}
	})
}
