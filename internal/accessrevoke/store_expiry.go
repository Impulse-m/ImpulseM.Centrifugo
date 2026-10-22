package accessrevoke

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// expireBatch is the number of expiry pops per hold of a kind lock.
const expireBatch int = 256

// maxExpiryDelaySeconds caps the delay of one timer arm. An entry which expires later wakes the timer once a day, and
// that run arms again.
const maxExpiryDelaySeconds int64 = 86400

// expiryItem is one entry of an expiry order: the expiry of a key at the time it was queued.
type expiryItem struct {
	expireAt int64
	key      [16]byte
}

// expiryHeap is a min-heap of expiry items by expireAt, guarded by the lock of its kind. Every entry of the kind has an
// item in the heap. An item of a key which was unblocked stays until its pop or a compaction, so after every write and
// at the end of every removal pass the heap holds at most twice the entries of the kind plus one. Between two holds of
// one pass, the deletes of the pass shrink the entries and the heap by the same count and can leave the heap above
// that bound; the compaction after the pass restores it.
type expiryHeap []expiryItem

// Len returns the number of items.
func (h expiryHeap) Len() int {
	return len(h)
}

// Less orders the items by expireAt.
func (h expiryHeap) Less(i int, j int) bool {
	return h[i].expireAt < h[j].expireAt
}

// Swap exchanges two items.
func (h expiryHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push is for heap.Interface only, never called: the store uses push and pop.
func (h *expiryHeap) Push(x any) {
	*h = append(*h, x.(expiryItem))
}

// Pop is for heap.Interface only, never called: the store uses push and pop.
func (h *expiryHeap) Pop() any {
	last := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return last
}

// push adds an item. It appends and sifts the item up with heap.Fix, which boxes only the pointer to the heap.
func (h *expiryHeap) push(item expiryItem) {
	*h = append(*h, item)
	heap.Fix(h, len(*h)-1)
}

// pop removes and returns the earliest item. The caller checks that the heap is not empty.
func (h *expiryHeap) pop() expiryItem {
	root := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	if last > 0 {
		heap.Fix(h, 0)
	}
	return root
}

// blockedExpireAt returns the expiry of a blocked user, which is the stored value.
func blockedExpireAt(expireAt int64) int64 {
	return expireAt
}

// sessionExpireAt returns the expiry of a session revocation.
func sessionExpireAt(entry sessionEntry) int64 {
	return entry.expireAt
}

// userExpireAt returns the expiry of a user invalidation.
func userExpireAt(entry userEntry) int64 {
	return entry.expireAt
}

// kindRef is one kind of the store: its lock, its entries, its expiry order, the version of its entries, and the expiry
// of an entry. Every write of the kind bumps version under mu, so a rebuild can tell that a write came after its snapshot.
type kindRef[V any] struct {
	mu         *sync.RWMutex
	entries    *map[[16]byte]V
	order      *expiryHeap
	version    *uint64
	expireAtOf func(V) int64
}

// blockedKind returns the blocked kind of the store.
func (s *Store) blockedKind() kindRef[int64] {
	return kindRef[int64]{
		mu:         &s.blockedMu,
		entries:    &s.blocked,
		order:      &s.blockedExpiry,
		version:    &s.blockedVersion,
		expireAtOf: blockedExpireAt,
	}
}

// sessionsKind returns the sessions kind of the store.
func (s *Store) sessionsKind() kindRef[sessionEntry] {
	return kindRef[sessionEntry]{
		mu:         &s.sessionsMu,
		entries:    &s.sessions,
		order:      &s.sessionsExpiry,
		version:    &s.sessionsVersion,
		expireAtOf: sessionExpireAt,
	}
}

// usersKind returns the users kind of the store.
func (s *Store) usersKind() kindRef[userEntry] {
	return kindRef[userEntry]{
		mu:         &s.usersMu,
		entries:    &s.users,
		order:      &s.usersExpiry,
		version:    &s.usersVersion,
		expireAtOf: userExpireAt,
	}
}

// expireKind pops the due items of one kind, at most expireBatch per hold of its lock, and settles each: the item of
// an absent key is stale and dropped, an expired entry is deleted, and an extended entry is queued again at its expiry.
// It pops at most the length the order had at its first hold, so writers cannot keep it going. It does not compact:
// compactKind does, once the pass has released the lock. It reads *entries and *order after every hold, because a
// compaction between two holds replaces them.
// It returns the earliest remaining expiry, and false when the order is empty.
func expireKind[V any](kind kindRef[V], now int64) (int64, bool) {
	kind.mu.Lock()
	defer kind.mu.Unlock()
	budget := len(*kind.order)
	held := 0
	for budget > 0 && len(*kind.order) > 0 && (*kind.order)[0].expireAt <= now {
		if held == expireBatch {
			kind.mu.Unlock()
			kind.mu.Lock()
			held = 0
			continue
		}
		item := kind.order.pop()
		*kind.version++
		budget--
		held++
		entry, ok := (*kind.entries)[item.key]
		if !ok {
			continue
		}
		if expireAt := kind.expireAtOf(entry); expireAt > now {
			kind.order.push(expiryItem{expireAt: expireAt, key: item.key})
			continue
		}
		delete(*kind.entries, item.key)
	}
	if len(*kind.order) == 0 {
		return 0, false
	}
	return (*kind.order)[0].expireAt, true
}

// compactAttempts bounds the rebuilds of one compaction. A write between a rebuild and its swap skips the swap, so the
// rebuild runs again, up to this many times. Under steady writes the swap can be skipped on every attempt; the stale
// items then stay until a pass finds the kind without a write in between.
const compactAttempts int = 3

// compactKind rebuilds the map and the order of a kind when the stale items of the order outnumber the entries: a new
// map and a new order sized to the entries, one item per entry at its expiry. A Go map never shrinks, so the new map
// gives back the capacity of the old peak and keeps the next walk proportional to the entries of this one.
// It is called by the expiry pass only, on the sweeper goroutine, never by a write, so a write does not wait for it.
// The caller does not hold the lock of the kind.
func compactKind[V any](kind kindRef[V]) {
	for range compactAttempts {
		if compactOnce(kind) {
			return
		}
	}
}

// compactOnce is one attempt of compactKind. The rebuild runs under the read lock, so checks go on meanwhile. A writer
// which is queued during the rebuild runs right after the read lock is released, and the readers after it wait for it, so
// a check can wait for that writer. Only the swap takes the write lock. A write between the snapshot and the swap makes
// the swap skip, and compactOnce reports false. It reports true when no compaction was due or the swap was made.
func compactOnce[V any](kind kindRef[V]) bool {
	kind.mu.RLock()
	if len(*kind.order) <= 2*len(*kind.entries) {
		kind.mu.RUnlock()
		return true
	}
	version := *kind.version
	fresh, live := rebuildKind(*kind.entries, kind.expireAtOf)
	if hook := compactTestHook.Load(); hook != nil {
		(*hook)("rebuilt")
	}
	kind.mu.RUnlock()

	if hook := compactTestHook.Load(); hook != nil {
		(*hook)("swap")
	}
	kind.mu.Lock()
	defer kind.mu.Unlock()
	if *kind.version != version {
		return false
	}
	*kind.entries = fresh
	*kind.order = live
	return true
}

// compactTestHook is a test seam: compactOnce calls it by stage, "rebuilt" under the read lock once the rebuild is built,
// and "swap" after the read lock is released and before the write lock is taken. Tests set it, production leaves it nil,
// and the cost in production is two atomic loads per attempt.
var compactTestHook atomic.Pointer[func(stage string)]

// rebuildKind returns a new map of the entries and a new expiry order with one item per entry at its expiry.
func rebuildKind[V any](entries map[[16]byte]V, expireAtOf func(V) int64) (map[[16]byte]V, expiryHeap) {
	fresh := make(map[[16]byte]V, len(entries))
	var live expiryHeap
	if len(entries) > 0 {
		live = make(expiryHeap, 0, len(entries))
	}
	for key, entry := range entries {
		fresh[key] = entry
		live = append(live, expiryItem{expireAt: expireAtOf(entry), key: key})
	}
	heap.Init(&live)
	return fresh, live
}

// earliestOf returns the earlier of two expiries, where a false ok means that there is none.
func earliestOf(a int64, aOK bool, b int64, bOK bool) (int64, bool) {
	if !aOK {
		return b, bOK
	}
	if !bOK || a <= b {
		return a, true
	}
	return b, true
}

// expireDue removes the due entries of every kind and returns the earliest remaining expiry, false when none is left.
func (s *Store) expireDue(now int64) (int64, bool) {
	blocked := s.blockedKind()
	blockedNext, blockedOK := expireKind(blocked, now)
	compactKind(blocked)
	sessions := s.sessionsKind()
	sessionsNext, sessionsOK := expireKind(sessions, now)
	compactKind(sessions)
	users := s.usersKind()
	usersNext, usersOK := expireKind(users, now)
	compactKind(users)
	next, ok := earliestOf(blockedNext, blockedOK, sessionsNext, sessionsOK)
	return earliestOf(next, ok, usersNext, usersOK)
}

// startExpiry turns the expiry timer on, with gap as the shortest time between two runs. It runs once for the entries
// written before the start, then arms for the earliest remaining one.
func (s *Store) startExpiry(gap time.Duration) {
	s.expiryMu.Lock()
	s.expiryOn = true
	s.expiryGap = gap
	s.expiryMu.Unlock()
	s.armExpiry(s.nowUnix())
}

// stopExpiry turns the expiry timer off and stops it. A run which has started completes and arms nothing.
func (s *Store) stopExpiry() {
	s.expiryMu.Lock()
	defer s.expiryMu.Unlock()
	s.expiryOn = false
	s.expiryAt = time.Time{}
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
	}
}

// armExpiry arms the expiry timer, while it is on, for an entry which expires at expireAt (Unix seconds): for that
// time, or expiryGap after the start of the previous run if that is later. A timer armed for an earlier time stays.
// The delay is read from the wall clock once, at arm time, and then runs on the monotonic clock. After a forward step
// of the wall clock, or a suspend of the host during which timers do not advance, an entry which expired by the wall
// clock stays in memory until the armed instant, which maxExpiryDelaySeconds bounds. This is a known limit: only memory
// and store_size see it, because Check compares with nowUnix and ignores expired entries.
func (s *Store) armExpiry(expireAt int64) {
	s.expiryMu.Lock()
	defer s.expiryMu.Unlock()
	if !s.expiryOn {
		return
	}
	seconds := min(max(expireAt-s.nowUnix(), 0), maxExpiryDelaySeconds)
	var at time.Time
	at = time.Now().Add(time.Duration(seconds) * time.Second)
	if earliest := s.expiryLast.Add(s.expiryGap); at.Before(earliest) {
		at = earliest
	}
	if !s.expiryAt.IsZero() && !s.expiryAt.After(at) {
		return
	}
	s.expiryAt = at
	if s.expiryTimer == nil {
		s.expiryTimer = time.AfterFunc(time.Until(at), s.runExpiry)
		return
	}
	s.expiryTimer.Reset(time.Until(at))
}

// runExpiry is the callback of the expiry timer: it removes the due entries and arms for the earliest remaining one.
// Two runs overlap only when one outlasts the gap; each step holds its kind lock, so an overlap is safe.
func (s *Store) runExpiry() {
	s.expiryMu.Lock()
	s.expiryAt = time.Time{}
	s.expiryLast = time.Now()
	s.expiryMu.Unlock()
	if next, ok := s.expireDue(s.nowUnix()); ok {
		s.armExpiry(next)
	}
}
