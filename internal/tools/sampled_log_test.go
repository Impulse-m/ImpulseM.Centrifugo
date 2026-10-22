package tools

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
// writes the count to its line and resets it, so it is reported once.
func TestSampledLogLogsSuppressedCountOnNextWarning(t *testing.T) {
	var logged bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&logged)
	t.Cleanup(func() { log.Logger = previous })
	sampled := &SampledLog{limiter: NewIntervalLimiter(0)}
	sampled.suppressed.Store(3)

	event := sampled.Event()
	require.NotNil(t, event)
	event.Msg("warned")

	require.Equal(t, int64(0), sampled.suppressed.Load())
	require.Contains(t, logged.String(), `"suppressed":3`)
}
