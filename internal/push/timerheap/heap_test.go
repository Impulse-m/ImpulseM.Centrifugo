package timerheap

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// entryCount is the number of entries of the large heap test, the size of a busy node's due heap.
const entryCount int = 10000

// fakeClock is a Clock which moves only when Advance is called. A timer runs its callback inside Advance when its
// time is reached, outside of the clock's lock, as a real timer runs on its own goroutine.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock   *fakeClock
	at      time.Time
	f       func()
	stopped bool
	fired   bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeTimer{clock: c, at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	wasPending := !t.stopped && !t.fired
	t.stopped = true
	return wasPending
}

// Advance moves the clock forward by d and runs the callbacks of the timers which are due.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []func()
	for _, t := range c.timers {
		if !t.stopped && !t.fired && !t.at.After(c.now) {
			t.fired = true
			due = append(due, t.f)
		}
	}
	c.mu.Unlock()

	for _, f := range due {
		f()
	}
}

// armed returns the timers which neither fired nor were stopped, that is the timers a heap would have running.
func (c *fakeClock) armed() []*fakeTimer {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*fakeTimer
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			result = append(result, t)
		}
	}
	return result
}

func TestHeapArmsOneTimerForTheEarliestDeadline(t *testing.T) {
	clock := newFakeClock()
	var fired int
	h := New[int](clock, func(int) {
		fired++
	})
	start := clock.Now()
	for i := 0; i < entryCount; i++ {
		h.Push(start.Add(time.Duration(entryCount-i)*time.Millisecond), i)
	}

	armed := clock.armed()
	require.Len(t, armed, 1)
	require.Equal(t, start.Add(time.Millisecond), armed[0].at)
	require.Equal(t, entryCount, h.Len())
	require.Zero(t, fired)
}

func TestHeapFiresEveryEntryOnceInDeadlineOrder(t *testing.T) {
	clock := newFakeClock()
	var order []int
	var mu sync.Mutex
	h := New[int](clock, func(value int) {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, value)
	})
	start := clock.Now()
	// The deadlines are distinct and pushed in a random order.
	indexes := rand.Perm(entryCount)
	for _, i := range indexes {
		h.Push(start.Add(time.Duration(i+1)*time.Millisecond), i)
	}

	clock.Advance(time.Millisecond - time.Nanosecond)
	require.Empty(t, order)

	clock.Advance(time.Nanosecond)
	require.Equal(t, []int{0}, order)
	armed := clock.armed()
	require.Len(t, armed, 1)
	require.Equal(t, start.Add(2*time.Millisecond), armed[0].at)

	clock.Advance(time.Duration(entryCount) * time.Millisecond)
	require.Len(t, order, entryCount)
	for i := 0; i < entryCount; i++ {
		require.Equal(t, i, order[i])
	}
	require.Zero(t, h.Len())
	require.Empty(t, clock.armed())
}

func TestHeapRearmsAfterRemovingTheHead(t *testing.T) {
	clock := newFakeClock()
	var fired []string
	h := New[string](clock, func(value string) {
		fired = append(fired, value)
	})
	start := clock.Now()
	head := h.Push(start.Add(time.Second), "head")
	h.Push(start.Add(2*time.Second), "next")

	require.True(t, h.Remove(head))
	armed := clock.armed()
	require.Len(t, armed, 1)
	require.Equal(t, start.Add(2*time.Second), armed[0].at)

	clock.Advance(time.Second)
	require.Empty(t, fired)
	clock.Advance(time.Second)
	require.Equal(t, []string{"next"}, fired)
}

func TestHeapRemoveOfNonHeadKeepsTheTimer(t *testing.T) {
	clock := newFakeClock()
	h := New[string](clock, func(string) {})
	start := clock.Now()
	h.Push(start.Add(time.Second), "head")
	middle := h.Push(start.Add(2*time.Second), "middle")

	require.True(t, h.Remove(middle))
	armed := clock.armed()
	require.Len(t, armed, 1)
	require.Equal(t, start.Add(time.Second), armed[0].at)
}

func TestHeapNeverArmsForAnEmptyHeap(t *testing.T) {
	clock := newFakeClock()
	var fired int
	h := New[int](clock, func(int) {
		fired++
	})
	start := clock.Now()
	first := h.Push(start.Add(time.Second), 1)
	second := h.Push(start.Add(2*time.Second), 2)

	require.True(t, h.Remove(first))
	require.True(t, h.Remove(second))
	require.Empty(t, clock.armed())

	clock.Advance(time.Hour)
	require.Zero(t, fired)
	require.Zero(t, h.Len())
}

func TestHeapRemoveReportsAbsentEntries(t *testing.T) {
	clock := newFakeClock()
	h := New[int](clock, func(int) {})
	other := New[int](clock, func(int) {})
	start := clock.Now()
	item := h.Push(start.Add(time.Second), 1)

	require.True(t, h.Remove(item))
	require.False(t, h.Remove(item))
	require.False(t, other.Remove(h.Push(start.Add(time.Second), 2)))
}

func TestHeapPeekReturnsTheEarliestEntry(t *testing.T) {
	clock := newFakeClock()
	h := New[string](clock, func(string) {})
	start := clock.Now()

	_, _, ok := h.Peek()
	require.False(t, ok)

	h.Push(start.Add(2*time.Second), "later")
	h.Push(start.Add(time.Second), "earlier")

	deadline, value, ok := h.Peek()
	require.True(t, ok)
	require.Equal(t, start.Add(time.Second), deadline)
	require.Equal(t, "earlier", value)
	require.Equal(t, 2, h.Len())
}

func TestHeapFiresWithTheSystemClock(t *testing.T) {
	fired := make(chan int, 1)
	h := New[int](SystemClock, func(value int) {
		fired <- value
	})

	h.Push(time.Now().Add(10*time.Millisecond), 7)

	select {
	case value := <-fired:
		require.Equal(t, 7, value)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the entry did not fire")
	}
}
