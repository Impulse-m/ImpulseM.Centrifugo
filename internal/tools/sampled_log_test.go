package tools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSampledLogCountsSuppressedWarnings lets one warning through and suppresses two more within the interval. The
// suppressed count is kept until the next warning is logged.
func TestSampledLogCountsSuppressedWarnings(t *testing.T) {
	sampled := &SampledLog{limiter: NewIntervalLimiter(time.Hour)}

	require.NotNil(t, sampled.Event())
	require.Nil(t, sampled.Event())
	require.Nil(t, sampled.Event())
	require.Equal(t, int64(2), sampled.suppressed.Load())
}

// TestSampledLogLogsSuppressedCountOnNextWarning sets a count of suppressed warnings, and checks that the next warning
// takes it and resets it, so it is reported once.
func TestSampledLogLogsSuppressedCountOnNextWarning(t *testing.T) {
	sampled := &SampledLog{limiter: NewIntervalLimiter(0)}
	sampled.suppressed.Store(3)

	require.NotNil(t, sampled.Event())
	require.Equal(t, int64(0), sampled.suppressed.Load())
}
