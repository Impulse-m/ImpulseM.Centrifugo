package accessrevoke

import (
	"container/list"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// minReadinessCheck is the shortest delay until the next readiness check, so that a check a moment early cannot spin.
const minReadinessCheck time.Duration = time.Millisecond

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
	// behindElem is the place of the partition in ReadinessTracker.behindOrder, nil while it is not behind.
	behindElem *list.Element
}

// lag returns the number of records between the consumed offset and the target offset of the partition.
func (p *partitionProgress) lag() int64 {
	if p.target > p.next {
		return p.target - p.next
	}
	return 0
}

// ReadinessTracker decides whether a source of access revocation events has caught up and whether it
// is lagging. The target offset of a partition is its last stable offset, so an open transaction does
// not count as lag. It is fed by per-partition observations (next consumed offset and target offset)
// and by successful fetches. It is safe for concurrent use.
type ReadinessTracker struct {
	// readyFlag and laggingFlag mirror Snapshot().Ready and Snapshot().Lagging. They are refreshed under mu
	// by every state change and by Snapshot, so Ready and Lagging are plain loads on the connect path.
	readyFlag       atomic.Bool
	laggingFlag     atomic.Bool
	lagRecordsValue atomic.Int64
	mu              sync.Mutex
	config          ReadinessConfig
	clock           func() time.Time
	startedAt       time.Time
	lastFetch       time.Time
	partitions      map[int32]*partitionProgress
	ready           bool
	failedOpen      bool
	caughtUp        bool
	caughtUpAt      time.Time
	caughtUpCh      chan struct{}
	closed          bool
	// notReached is the number of partitions which never reached their target, lagRecords the sum of their lags,
	// behind the number of partitions with a behindSince, and oldestBehind the earliest of those. They are kept
	// up to date by Observe, so evaluating the state does not loop over the partitions.
	notReached   int
	lagRecords   int64
	behind       int
	oldestBehind time.Time
	// behindOrder holds the partitions which are behind, in the order they became behind, so its front is the oldest one.
	behindOrder list.List
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
		t.notReached++
	}
	t.lagRecords -= progress.lag()
	if next >= 0 {
		progress.next = next
	}
	progress.target = target
	t.lagRecords += progress.lag()
	if progress.next >= progress.target {
		t.reachedLocked(progress)
	} else {
		t.behindLocked(progress, now)
	}
	t.refreshLocked(now)
}

// reachedLocked records that the partition is at its target, which ends its lag.
func (t *ReadinessTracker) reachedLocked(progress *partitionProgress) {
	if !progress.reached {
		progress.reached = true
		t.notReached--
	}
	if progress.behindElem == nil {
		return
	}
	t.behindOrder.Remove(progress.behindElem)
	progress.behindElem = nil
	progress.behindSince = time.Time{}
	t.behind--
	t.updateOldestBehindLocked()
}

// behindLocked records that the partition is behind its target, starting its lag at now unless it is behind already.
// The clock does not go back, so a partition which becomes behind is never older than the ones before it in behindOrder.
func (t *ReadinessTracker) behindLocked(progress *partitionProgress, now time.Time) {
	if progress.behindElem != nil {
		return
	}
	progress.behindSince = now
	progress.behindElem = t.behindOrder.PushBack(progress)
	t.behind++
	t.updateOldestBehindLocked()
}

// updateOldestBehindLocked sets oldestBehind to the behindSince of the front of behindOrder, the partition which has
// been behind the longest. Removing a partition from the middle of the order keeps the rest in time order, so this is
// O(1) at every transition and no loop over the partitions is needed.
func (t *ReadinessTracker) updateOldestBehindLocked() {
	t.oldestBehind = time.Time{}
	if front := t.behindOrder.Front(); front != nil {
		t.oldestBehind = front.Value.(*partitionProgress).behindSince
	}
}

// FetchSucceeded records a successful fetch response from a broker, even an empty one.
func (t *ReadinessTracker) FetchSucceeded() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	t.lastFetch = now
	t.refreshLocked(now)
}

// Snapshot evaluates the current state. It also refreshes the flags read by Ready, Lagging and LagRecords, so the
// time based transitions (startup timeout, lag threshold) become visible to them on every call. A source refreshes
// them at each of their deadlines with snapshotWithNextCheck.
func (t *ReadinessTracker) Snapshot() ReadinessSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refreshLocked(t.clock())
}

// snapshotWithNextCheck evaluates the current state like Snapshot, and returns the delay until the next time based
// transition, see nextCheckLocked.
func (t *ReadinessTracker) snapshotWithNextCheck() (ReadinessSnapshot, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	snapshot := t.refreshLocked(now)
	return snapshot, t.nextCheckLocked(now)
}

// Ready reports whether the tracker is ready. See ReadinessSnapshot.Ready. It is a lock free load, the
// value is refreshed by Observe, FetchSucceeded and Snapshot.
func (t *ReadinessTracker) Ready() bool {
	return t.readyFlag.Load()
}

// Lagging reports whether the tracker is lagging. See ReadinessSnapshot.Lagging. It is a lock free load,
// the value is refreshed by Observe, FetchSucceeded and Snapshot.
func (t *ReadinessTracker) Lagging() bool {
	return t.laggingFlag.Load()
}

// LagRecords returns the number of records between consumed and target offsets. See ReadinessSnapshot.LagRecords. It
// is a lock free load, the value is refreshed by Observe, FetchSucceeded and Snapshot.
func (t *ReadinessTracker) LagRecords() int64 {
	return t.lagRecordsValue.Load()
}

// nextCheckLocked returns the time from now until the earliest time based transition which has not happened yet: the
// startup timeout while not ready and not strict, and each lag rule while it does not hold. When none is pending it
// returns the lag threshold, so that a lag which starts after a recovery is measured again. The result is at least
// minReadinessCheck. The caller holds mu and has refreshed the state for now.
func (t *ReadinessTracker) nextCheckLocked(now time.Time) time.Duration {
	var next time.Duration = math.MaxInt64
	if !t.ready && !t.config.Strict {
		next = min(next, t.startedAt.Add(t.config.StartupTimeout).Sub(now))
	}
	if silent := now.Sub(t.lastFetch); silent <= t.config.LagThreshold {
		next = min(next, t.config.LagThreshold-silent+time.Nanosecond)
	}
	if !t.oldestBehind.IsZero() {
		if behind := now.Sub(t.oldestBehind); behind <= t.config.LagThreshold {
			next = min(next, t.config.LagThreshold-behind+time.Nanosecond)
		}
	}
	if next == math.MaxInt64 {
		next = t.config.LagThreshold
	}
	return max(next, minReadinessCheck)
}

// refreshLocked evaluates the state for the given moment, publishes Ready, Lagging and LagRecords to their flags and
// returns the snapshot.
func (t *ReadinessTracker) refreshLocked(now time.Time) ReadinessSnapshot {
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
	snapshot.LagRecords = t.lagRecords
	if !t.oldestBehind.IsZero() && now.Sub(t.oldestBehind) > t.config.LagThreshold {
		snapshot.Lagging = true
	}
	t.readyFlag.Store(snapshot.Ready)
	t.laggingFlag.Store(snapshot.Lagging)
	t.lagRecordsValue.Store(snapshot.LagRecords)
	return snapshot
}

// CaughtUpAfterFailOpen returns a channel which is closed once the tracker catches up after it became
// ready by startup timeout. The channel is never closed if the tracker caught up before the timeout.
func (t *ReadinessTracker) CaughtUpAfterFailOpen() <-chan struct{} {
	return t.caughtUpCh
}

// evaluateLocked updates ready, failedOpen and caughtUp flags for the given moment. No partition known yet means
// the offsets are not listed yet, so it is not caught up.
func (t *ReadinessTracker) evaluateLocked(now time.Time) {
	if !t.caughtUp && len(t.partitions) > 0 && t.notReached == 0 {
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
