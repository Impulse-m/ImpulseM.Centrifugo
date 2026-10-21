package accessrevoke

import (
	"container/heap"
	"sync"
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
// that bound; the compaction check under the last hold restores it.
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

// expireKind pops the due items of one kind, at most expireBatch per hold of its lock, and settles each: the item of
// an absent key is stale and dropped, an expired entry is deleted, and an extended entry is queued again at its expiry.
// It pops at most the length the order had at its first hold, so writers cannot keep it going, and compacts under its
// last hold. It reads *entries and *order after every hold, because a compaction by UnblockUser between two holds
// replaces them.
// It returns the earliest remaining expiry, and false when the order is empty.
func expireKind[V any](
	mu *sync.RWMutex,
	entries *map[[16]byte]V,
	order *expiryHeap,
	expireAtOf func(V) int64,
	now int64,
) (int64, bool) {
	mu.Lock()
	defer mu.Unlock()
	budget := len(*order)
	held := 0
	for budget > 0 && len(*order) > 0 && (*order)[0].expireAt <= now {
		if held == expireBatch {
			mu.Unlock()
			mu.Lock()
			held = 0
			continue
		}
		item := order.pop()
		budget--
		held++
		entry, ok := (*entries)[item.key]
		if !ok {
			continue
		}
		if expireAt := expireAtOf(entry); expireAt > now {
			order.push(expiryItem{expireAt: expireAt, key: item.key})
			continue
		}
		delete(*entries, item.key)
	}
	compactIfStale(entries, order, expireAtOf)
	if len(*order) == 0 {
		return 0, false
	}
	return (*order)[0].expireAt, true
}

// compactIfStale rebuilds the map and the order of a kind when the stale items of the order outnumber the entries: a
// new map and a new order sized to the entries, one item per entry at its expiry. A Go map never shrinks, so the new
// map gives back the capacity of the old peak and keeps the next walk proportional to the entries of this one. The
// caller holds the write lock of the kind.
func compactIfStale[V any](entries *map[[16]byte]V, order *expiryHeap, expireAtOf func(V) int64) {
	if len(*order) <= 2*len(*entries) {
		return
	}
	fresh := make(map[[16]byte]V, len(*entries))
	var live expiryHeap
	if len(*entries) > 0 {
		live = make(expiryHeap, 0, len(*entries))
	}
	for key, entry := range *entries {
		fresh[key] = entry
		live = append(live, expiryItem{expireAt: expireAtOf(entry), key: key})
	}
	heap.Init(&live)
	*entries = fresh
	*order = live
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
	blockedNext, blockedOK := expireKind(&s.blockedMu, &s.blocked, &s.blockedExpiry, blockedExpireAt, now)
	sessionsNext, sessionsOK := expireKind(&s.sessionsMu, &s.sessions, &s.sessionsExpiry, sessionExpireAt, now)
	usersNext, usersOK := expireKind(&s.usersMu, &s.users, &s.usersExpiry, userExpireAt, now)
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
