package timerheap

import (
	"container/heap"
	"sync"
	"time"
)

// Clock is the time source of a Heap. SystemClock is the clock of the process, tests inject their own.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a call of Clock.AfterFunc which has not run yet.
type Timer interface {
	Stop() bool
}

// SystemClock is the Clock of the process, backed by the time package.
var SystemClock Clock = systemClock{}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}

// Item is an entry of a Heap. Push returns it, so that the entry can be removed with Remove.
type Item[T any] struct {
	deadline time.Time
	value    T
	index    int
}

// Heap is a deadline heap which runs one timer for the whole heap: the timer is armed for the earliest deadline only,
// so an entry costs no goroutine, no timer and no periodic tick. When the timer fires, every entry which is due is
// removed and handed to the fire callback in deadline order. A Heap is safe for concurrent use. The callbacks of one
// firing run in order, but a later firing may run its callbacks concurrently with them.
type Heap[T any] struct {
	mu         sync.Mutex
	clock      Clock
	fire       func(T)
	items      entryHeap[T]
	timer      Timer
	generation uint64
}

// New returns an empty Heap which reads time from clock and hands every due entry to fire.
func New[T any](clock Clock, fire func(T)) *Heap[T] {
	return &Heap[T]{
		clock: clock,
		fire:  fire,
	}
}

// Push adds an entry which is due at deadline and returns its handle for Remove.
func (h *Heap[T]) Push(deadline time.Time, value T) *Item[T] {
	h.mu.Lock()
	defer h.mu.Unlock()

	item := &Item[T]{deadline: deadline, value: value}
	heap.Push(&h.items, item)
	if item.index == 0 {
		h.rearm()
	}
	return item
}

// Remove drops an entry which has not fired yet. It reports false when the entry is not in this heap any more.
func (h *Heap[T]) Remove(item *Item[T]) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if item.index < 0 || item.index >= len(h.items) || h.items[item.index] != item {
		return false
	}
	wasHead := item.index == 0
	heap.Remove(&h.items, item.index)
	if wasHead {
		h.rearm()
	}
	return true
}

// Peek returns the earliest deadline and its value without removing the entry. ok is false when the heap is empty.
func (h *Heap[T]) Peek() (deadline time.Time, value T, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.items) == 0 {
		return deadline, value, false
	}
	return h.items[0].deadline, h.items[0].value, true
}

// Len returns the number of entries which have not fired yet.
func (h *Heap[T]) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.items)
}

// rearm stops the armed timer, if any, and arms a new one for the earliest deadline. An empty heap has no timer.
// The caller holds mu.
func (h *Heap[T]) rearm() {
	if h.timer != nil {
		h.timer.Stop()
		h.timer = nil
	}
	h.generation++
	if len(h.items) == 0 {
		return
	}
	generation := h.generation
	delay := h.items[0].deadline.Sub(h.clock.Now())
	h.timer = h.clock.AfterFunc(delay, func() {
		h.onTimer(generation)
	})
}

// onTimer runs when the armed timer fires. A timer which was replaced by rearm after it had fired does nothing.
func (h *Heap[T]) onTimer(generation uint64) {
	h.mu.Lock()
	if generation != h.generation {
		h.mu.Unlock()
		return
	}
	h.timer = nil
	now := h.clock.Now()
	var due []T
	for len(h.items) > 0 && !h.items[0].deadline.After(now) {
		item := heap.Pop(&h.items).(*Item[T])
		due = append(due, item.value)
	}
	h.rearm()
	h.mu.Unlock()

	for _, value := range due {
		h.fire(value)
	}
}

// entryHeap is the binary heap of entries ordered by deadline, it implements heap.Interface and keeps the index of
// every entry up to date for Remove.
type entryHeap[T any] []*Item[T]

func (s entryHeap[T]) Len() int {
	return len(s)
}

func (s entryHeap[T]) Less(i int, j int) bool {
	return s[i].deadline.Before(s[j].deadline)
}

func (s entryHeap[T]) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].index = i
	s[j].index = j
}

func (s *entryHeap[T]) Push(x any) {
	item := x.(*Item[T])
	item.index = len(*s)
	*s = append(*s, item)
}

func (s *entryHeap[T]) Pop() any {
	old := *s
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*s = old[:last]
	return item
}
