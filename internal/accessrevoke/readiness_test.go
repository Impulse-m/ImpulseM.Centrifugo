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
	// Time based transitions are published by Snapshot, which the watch loop calls every tick.
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
	// Time based transitions are published by Snapshot, which the watch loop calls every tick.
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
