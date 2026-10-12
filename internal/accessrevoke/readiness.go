package accessrevoke

import (
	"sync"
	"time"
)

// ReadinessConfig configures a ReadinessTracker.
type ReadinessConfig struct {
	// StartupTimeout is how long the tracker waits to catch up before it becomes ready anyway.
	StartupTimeout time.Duration
	// LagThreshold is how long a partition may stay behind, or fetches may stop, before lagging.
	LagThreshold time.Duration
	// Strict disables startup timeout: the tracker waits for catch up indefinitely.
	Strict bool
}

// ReadinessSnapshot is a state of a ReadinessTracker at some moment.
type ReadinessSnapshot struct {
	// Ready is true once every partition reached its target offset at least once, or after startup
	// timeout when not strict. Ready stays true afterwards.
	Ready bool
	// FailedOpen is true when Ready was set by startup timeout before catch up.
	FailedOpen bool
	// CaughtUp is true once every partition reached its target offset at least once.
	CaughtUp bool
	// Lagging is true when a partition stayed behind longer than LagThreshold, or no fetch succeeded
	// for longer than LagThreshold.
	Lagging bool
	// LagRecords is the number of records between consumed offsets and target offsets.
	LagRecords int64
	// CatchUpDuration is the time from start until CaughtUp. Zero when not caught up yet.
	CatchUpDuration time.Duration
}

// partitionProgress is the progress of a single partition.
type partitionProgress struct {
	next        int64
	target      int64
	reached     bool
	behindSince time.Time
}

// ReadinessTracker decides whether a source of access revocation events has caught up and whether it
// is lagging. The target offset of a partition is its last stable offset, so an open transaction does
// not count as lag. It is fed by per-partition observations (next consumed offset and target offset)
// and by successful fetches. It is safe for concurrent use.
type ReadinessTracker struct {
	mu         sync.Mutex
	config     ReadinessConfig
	clock      func() time.Time
	startedAt  time.Time
	lastFetch  time.Time
	partitions map[int32]*partitionProgress
	ready      bool
	failedOpen bool
	caughtUp   bool
	caughtUpAt time.Time
	caughtUpCh chan struct{}
	closed     bool
}

// NewReadinessTracker creates a ReadinessTracker. The clock is used for all timing decisions, so tests
// can pass a fake one. Startup is measured from the moment of creation.
func NewReadinessTracker(config ReadinessConfig, clock func() time.Time) *ReadinessTracker {
	now := clock()
	return &ReadinessTracker{
		config:     config,
		clock:      clock,
		startedAt:  now,
		lastFetch:  now,
		partitions: make(map[int32]*partitionProgress),
		caughtUpCh: make(chan struct{}),
	}
}

// Observe records the next offset to be consumed and the target offset of a partition. A negative next
// keeps the previously observed next offset, which is used when a fetch returns the target offset
// of a partition without records.
func (t *ReadinessTracker) Observe(partition int32, next int64, target int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	progress, ok := t.partitions[partition]
	if !ok {
		progress = &partitionProgress{}
		t.partitions[partition] = progress
	}
	if next >= 0 {
		progress.next = next
	}
	progress.target = target
	if progress.next >= progress.target {
		progress.reached = true
		progress.behindSince = time.Time{}
	} else if progress.behindSince.IsZero() {
		progress.behindSince = now
	}
	t.evaluateLocked(now)
}

// FetchSucceeded records a successful fetch response from a broker, even an empty one.
func (t *ReadinessTracker) FetchSucceeded() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastFetch = t.clock()
}

// Snapshot evaluates the current state.
func (t *ReadinessTracker) Snapshot() ReadinessSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	t.evaluateLocked(now)

	snapshot := ReadinessSnapshot{
		Ready:      t.ready,
		FailedOpen: t.failedOpen,
		CaughtUp:   t.caughtUp,
	}
	if t.caughtUp {
		snapshot.CatchUpDuration = t.caughtUpAt.Sub(t.startedAt)
	}
	snapshot.Lagging = now.Sub(t.lastFetch) > t.config.LagThreshold
	for _, progress := range t.partitions {
		if progress.target > progress.next {
			snapshot.LagRecords += progress.target - progress.next
		}
		if !progress.behindSince.IsZero() && now.Sub(progress.behindSince) > t.config.LagThreshold {
			snapshot.Lagging = true
		}
	}
	return snapshot
}

// Ready reports whether the tracker is ready. See ReadinessSnapshot.Ready.
func (t *ReadinessTracker) Ready() bool {
	return t.Snapshot().Ready
}

// Lagging reports whether the tracker is lagging. See ReadinessSnapshot.Lagging.
func (t *ReadinessTracker) Lagging() bool {
	return t.Snapshot().Lagging
}

// CaughtUpAfterFailOpen returns a channel which is closed once the tracker catches up after it became
// ready by startup timeout. The channel is never closed if the tracker caught up before the timeout.
func (t *ReadinessTracker) CaughtUpAfterFailOpen() <-chan struct{} {
	return t.caughtUpCh
}

// evaluateLocked updates ready, failedOpen and caughtUp flags for the given moment.
func (t *ReadinessTracker) evaluateLocked(now time.Time) {
	if !t.caughtUp && t.allReachedLocked() {
		t.caughtUp = true
		t.caughtUpAt = now
		if t.failedOpen && !t.closed {
			t.closed = true
			close(t.caughtUpCh)
		}
	}
	if t.ready {
		return
	}
	if t.caughtUp {
		t.ready = true
		return
	}
	if !t.config.Strict && now.Sub(t.startedAt) >= t.config.StartupTimeout {
		t.ready = true
		t.failedOpen = true
	}
}

// allReachedLocked reports whether every known partition reached its target offset at least once. No
// partition known yet means the offsets are not listed yet, so it is not caught up.
func (t *ReadinessTracker) allReachedLocked() bool {
	if len(t.partitions) == 0 {
		return false
	}
	for _, progress := range t.partitions {
		if !progress.reached {
			return false
		}
	}
	return true
}
