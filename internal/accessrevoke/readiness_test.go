package accessrevoke

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeReadinessClock is a manually advanced clock.
type fakeReadinessClock struct {
	now time.Time
}

func (c *fakeReadinessClock) Now() time.Time {
	return c.now
}

func (c *fakeReadinessClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func newReadinessTest(strict bool) (*ReadinessTracker, *fakeReadinessClock) {
	clock := &fakeReadinessClock{now: time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)}
	tracker := NewReadinessTracker(ReadinessConfig{
		StartupTimeout: time.Minute,
		LagThreshold:   30 * time.Second,
		Strict:         strict,
	}, clock.Now)
	return tracker, clock
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestReadinessReadyWhenAllPartitionsReachHighWatermark(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	tracker.Observe(1, 5, 5)

	require.False(t, tracker.Ready())

	tracker.Observe(0, 10, 10)
	snapshot := tracker.Snapshot()
	require.True(t, snapshot.Ready)
	require.True(t, snapshot.CaughtUp)
	require.False(t, snapshot.FailedOpen)
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))
}

func TestReadinessIdlePartitionWithNoRecordsIsCaughtUp(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	// Partition with nothing in window: next offset equals high watermark from the start.
	tracker.Observe(0, 7, 7)
	require.True(t, tracker.Ready())
}

func TestReadinessNotReadyBeforeAnyPartitionIsObserved(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	// Offsets are not listed yet: nothing is known, so the tracker is not caught up.
	require.False(t, tracker.Ready())
	require.False(t, tracker.Snapshot().CaughtUp)
}

func TestReadinessEmptyHighWatermarkOnlyFetchKeepsNext(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	tracker.Observe(0, 3, 10)
	// Fetch returned HWM without records: next offset stays at 3.
	tracker.Observe(0, -1, 12)
	require.False(t, tracker.Ready())
	require.Equal(t, int64(9), tracker.Snapshot().LagRecords)

	tracker.Observe(0, 12, 12)
	require.True(t, tracker.Ready())
}

func TestReadinessStartupTimeoutFailsOpen(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)

	clock.Advance(59 * time.Second)
	require.False(t, tracker.Ready())

	clock.Advance(2 * time.Second)
	snapshot := tracker.Snapshot()
	require.True(t, snapshot.Ready)
	require.True(t, snapshot.FailedOpen)
	require.False(t, snapshot.CaughtUp)
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))
}

func TestReadinessCaughtUpAfterFailOpenSignalsOnce(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	clock.Advance(2 * time.Minute)
	// Time based transitions are published by Snapshot, which the source's readiness timer calls at each transition.
	tracker.Snapshot()
	require.True(t, tracker.Ready())
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))

	tracker.Observe(0, 10, 10)
	require.True(t, channelClosed(tracker.CaughtUpAfterFailOpen()))

	// Stays ready, no more transitions.
	tracker.Observe(0, 12, 12)
	snapshot := tracker.Snapshot()
	require.True(t, snapshot.Ready)
	require.True(t, snapshot.CaughtUp)
	require.True(t, snapshot.FailedOpen)
	require.Equal(t, time.Duration(2*time.Minute), snapshot.CatchUpDuration)
}

func TestReadinessCaughtUpBeforeTimeoutDoesNotSignal(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	clock.Advance(10 * time.Second)
	tracker.Observe(0, 10, 10)

	clock.Advance(5 * time.Minute)
	snapshot := tracker.Snapshot()
	require.True(t, snapshot.Ready)
	require.False(t, snapshot.FailedOpen)
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))
	require.Equal(t, 10*time.Second, snapshot.CatchUpDuration)
}

func TestReadinessStrictWaitsIndefinitely(t *testing.T) {
	tracker, clock := newReadinessTest(true)
	tracker.Observe(0, 0, 10)

	clock.Advance(time.Hour)
	snapshot := tracker.Snapshot()
	require.False(t, snapshot.Ready)
	require.False(t, snapshot.FailedOpen)
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))

	tracker.Observe(0, 10, 10)
	snapshot = tracker.Snapshot()
	require.True(t, snapshot.Ready)
	require.False(t, snapshot.FailedOpen)
	// Caught up without fail-open: no signal.
	require.False(t, channelClosed(tracker.CaughtUpAfterFailOpen()))
}

func TestReadinessLaggingWhenPartitionStaysBehind(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	tracker.Observe(0, 10, 10)
	require.False(t, tracker.Lagging())

	// New records appear and are not consumed for longer than the lag threshold.
	tracker.FetchSucceeded()
	tracker.Observe(0, -1, 20)
	clock.Advance(20 * time.Second)
	tracker.FetchSucceeded()
	require.False(t, tracker.Lagging())

	clock.Advance(15 * time.Second)
	tracker.FetchSucceeded()
	require.True(t, tracker.Lagging())
	require.Equal(t, int64(10), tracker.Snapshot().LagRecords)

	// Catch up clears lagging.
	tracker.Observe(0, 20, 20)
	require.False(t, tracker.Lagging())
}

func TestReadinessLaggingWhenNoSuccessfulFetch(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 10, 10)

	clock.Advance(29 * time.Second)
	require.False(t, tracker.Lagging())

	clock.Advance(2 * time.Second)
	// Time based transitions are published by Snapshot, which the source's readiness timer calls at each transition.
	tracker.Snapshot()
	require.True(t, tracker.Lagging())

	// Idle topic with successful empty fetches is not lagging.
	tracker.FetchSucceeded()
	require.False(t, tracker.Lagging())
}

func TestReadinessLagRecordsSumsPartitions(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	tracker.Observe(0, 2, 5)
	tracker.Observe(1, 7, 10)
	tracker.Observe(2, 4, 4)
	require.Equal(t, int64(6), tracker.Snapshot().LagRecords)
}

// recomputedReadiness is the state of a ReadinessTracker which a loop over its partitions gives.
type recomputedReadiness struct {
	lagRecords   int64
	lagging      bool
	notReached   int
	behind       int
	oldestBehind time.Time
	allReached   bool
}

// recomputeReadiness loops over the partitions of tracker at now, the way the state was computed before the counters
// were incremental.
func recomputeReadiness(tracker *ReadinessTracker, now time.Time) recomputedReadiness {
	var state recomputedReadiness
	state.lagging = now.Sub(tracker.lastFetch) > tracker.config.LagThreshold
	for _, progress := range tracker.partitions {
		if progress.target > progress.next {
			state.lagRecords += progress.target - progress.next
		}
		if !progress.reached {
			state.notReached++
		}
		if progress.behindSince.IsZero() {
			continue
		}
		state.behind++
		if state.oldestBehind.IsZero() || progress.behindSince.Before(state.oldestBehind) {
			state.oldestBehind = progress.behindSince
		}
		if now.Sub(progress.behindSince) > tracker.config.LagThreshold {
			state.lagging = true
		}
	}
	state.allReached = len(tracker.partitions) > 0 && state.notReached == 0
	return state
}

// TestReadinessIncrementalMatchesRecompute runs a deterministic sequence of observations and fetches over 50 partitions.
// After each step the incremental state must equal what a loop over the partitions gives.
func TestReadinessIncrementalMatchesRecompute(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	state := uint64(1)
	nextRandom := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}
	caughtUp := false
	for range 2000 {
		if nextRandom()%4 == 0 {
			tracker.FetchSucceeded()
		} else {
			partition := int32(nextRandom() % 50)
			target := int64(nextRandom() % 1000)
			consumed := int64(nextRandom()%1100) - 50
			tracker.Observe(partition, consumed, target)
		}
		clock.Advance(time.Duration(nextRandom()%4) * time.Second)

		snapshot := tracker.Snapshot()
		expected := recomputeReadiness(tracker, clock.now)
		caughtUp = caughtUp || expected.allReached
		require.Equal(t, expected.lagRecords, snapshot.LagRecords)
		require.Equal(t, expected.lagging, snapshot.Lagging)
		require.Equal(t, caughtUp, snapshot.CaughtUp)
		require.Equal(t, expected.notReached, tracker.notReached)
		require.Equal(t, expected.behind, tracker.behind)
		require.True(t, expected.oldestBehind.Equal(tracker.oldestBehind))
	}
}

// TestReadinessOldestBehindRecomputedWhenItClears makes three partitions fall behind one second apart. When the oldest
// of them catches up, the lag is measured from the next oldest one, not from the cleared one.
func TestReadinessOldestBehindRecomputedWhenItClears(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	clock.Advance(time.Second)
	tracker.Observe(1, 0, 10)
	clock.Advance(time.Second)
	tracker.Observe(2, 0, 10)
	clock.Advance(time.Second)
	tracker.Observe(0, 10, 10)
	require.False(t, tracker.Lagging())

	// The oldest partition left is the one behind since t1, so lagging starts after t1 plus the threshold.
	clock.Advance(28 * time.Second)
	tracker.FetchSucceeded()
	require.False(t, tracker.Lagging())

	clock.Advance(time.Second)
	tracker.FetchSucceeded()
	require.True(t, tracker.Lagging())

	// Partition 1 catches up, and partition 2, behind since t2, is the oldest one left.
	tracker.Observe(1, 10, 10)
	require.False(t, tracker.Lagging())

	clock.Advance(time.Second)
	tracker.FetchSucceeded()
	require.True(t, tracker.Lagging())
}

func TestReadinessNextCheckFollowsTransitions(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 0, 10)
	_, next := tracker.snapshotWithNextCheck()
	require.Equal(t, 30*time.Second+time.Nanosecond, next)

	clock.Advance(31 * time.Second)
	snapshot, next := tracker.snapshotWithNextCheck()
	require.True(t, snapshot.Lagging)
	require.Equal(t, 29*time.Second, next)

	clock.Advance(29 * time.Second)
	snapshot, next = tracker.snapshotWithNextCheck()
	require.True(t, snapshot.Ready)
	require.True(t, snapshot.FailedOpen)
	require.Equal(t, 30*time.Second, next)
}

func TestReadinessNextCheckSilenceBeforeStartup(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	_, next := tracker.snapshotWithNextCheck()
	require.Equal(t, 30*time.Second+time.Nanosecond, next)

	clock.Advance(30*time.Second + time.Nanosecond)
	snapshot, next := tracker.snapshotWithNextCheck()
	require.True(t, snapshot.Lagging)
	require.False(t, snapshot.Ready)
	require.Equal(t, 29*time.Second+999999999*time.Nanosecond, next)
}

func TestReadinessNextCheckKeepsPartitionTermWhileSilent(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	clock.Advance(20 * time.Second)
	tracker.Observe(0, 0, 10)

	clock.Advance(11 * time.Second)
	snapshot, next := tracker.snapshotWithNextCheck()
	require.True(t, snapshot.Lagging)
	require.Equal(t, 19*time.Second+time.Nanosecond, next)

	clock.Advance(4 * time.Second)
	tracker.FetchSucceeded()
	require.False(t, tracker.Lagging())

	clock.Advance(15*time.Second + time.Nanosecond)
	snapshot, _ = tracker.snapshotWithNextCheck()
	require.True(t, snapshot.Lagging)
}

func TestReadinessNextCheckMovesWithFetch(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	clock.Advance(10 * time.Second)
	tracker.FetchSucceeded()
	_, next := tracker.snapshotWithNextCheck()
	require.Equal(t, 30*time.Second+time.Nanosecond, next)
}

func TestReadinessNextCheckUsesOldestBehind(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 10, 10)
	clock.Advance(5 * time.Second)
	tracker.Observe(1, 0, 10)
	clock.Advance(15 * time.Second)
	tracker.FetchSucceeded()
	_, next := tracker.snapshotWithNextCheck()
	require.Equal(t, 15*time.Second+time.Nanosecond, next)
}

func TestReadinessNextCheckStrictHasNoStartupTerm(t *testing.T) {
	tracker, clock := newReadinessTest(true)
	tracker.Observe(0, 0, 10)
	clock.Advance(31 * time.Second)
	_, next := tracker.snapshotWithNextCheck()
	require.Equal(t, 30*time.Second, next)
}

func TestReadinessNextCheckHasFloor(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	tracker.Observe(0, 10, 10)
	clock.Advance(30 * time.Second)
	snapshot, next := tracker.snapshotWithNextCheck()
	require.False(t, snapshot.Lagging)
	require.Equal(t, time.Millisecond, next)
}

func TestReadinessFailOpenIsSetByAnyRefresh(t *testing.T) {
	tracker, clock := newReadinessTest(false)
	clock.Advance(61 * time.Second)
	tracker.FetchSucceeded()
	require.True(t, tracker.Ready())

	snapshot := tracker.Snapshot()
	require.True(t, snapshot.FailedOpen)
}

func TestReadinessLagRecordsIsLockFree(t *testing.T) {
	tracker, _ := newReadinessTest(false)
	tracker.Observe(0, 2, 5)
	tracker.Observe(1, 7, 10)
	require.Equal(t, int64(6), tracker.LagRecords())
}

func BenchmarkReadinessObserve(b *testing.B) {
	tracker, _ := newReadinessTest(false)
	for partition := range 1000 {
		tracker.Observe(int32(partition), 0, 10)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		tracker.Observe(int32(i%1000), int64(i%10), 10)
	}
}
