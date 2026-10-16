package backoffpolicy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// samplesPerAttempt is the number of randomized delays drawn for each attempt of the schedule.
const samplesPerAttempt int = 200

func TestExponentialSetsFields(t *testing.T) {
	b := Exponential(10*time.Millisecond, 80*time.Millisecond)

	require.Equal(t, 10*time.Millisecond, b.InitialInterval)
	require.Equal(t, 80*time.Millisecond, b.MaxInterval)
	require.Equal(t, 2.0, b.Multiplier)
	require.Equal(t, 0.2, b.RandomizationFactor)
}

func TestExponentialScheduleIsExactWithoutRandomization(t *testing.T) {
	b := Exponential(10*time.Second, 10*time.Minute)
	b.RandomizationFactor = 0

	// The interval doubles on every attempt and stops at the maximum; without randomization each delay is the interval.
	schedule := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		320 * time.Second,
		10 * time.Minute,
		10 * time.Minute,
	}
	for attempt, expected := range schedule {
		require.Equal(t, expected, b.NextBackOff(), "attempt %d", attempt)
	}
}

func TestExponentialScheduleDoublesUpToMax(t *testing.T) {
	initial := 10 * time.Second
	maxInterval := 10 * time.Minute
	// The centres of the schedule: the initial interval doubles on every attempt and stops at the maximum.
	centres := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		320 * time.Second,
		10 * time.Minute,
		10 * time.Minute,
	}

	for attempt, centre := range centres {
		low := time.Duration(float64(centre) * 0.8)
		high := time.Duration(float64(centre) * 1.2)
		for sample := 0; sample < samplesPerAttempt; sample++ {
			b := Exponential(initial, maxInterval)
			var delay time.Duration
			for step := 0; step <= attempt; step++ {
				delay = b.NextBackOff()
			}
			require.GreaterOrEqual(t, delay, low, "attempt %d", attempt)
			require.LessOrEqual(t, delay, high, "attempt %d", attempt)
		}
	}
}
