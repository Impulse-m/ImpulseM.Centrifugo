package rpcregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPollStartPhaseStaysWithinOneInterval verifies the range of pollStartPhase, which is the start phase
// of a node within one interval, and that the start differs between nodes. It tests the helper only: the
// wait for the phase before the first check in poll is not observed here, since the phase is random.
func TestPollStartPhaseStaysWithinOneInterval(t *testing.T) {
	const interval = 5 * time.Second
	seen := make(map[time.Duration]bool)
	for range 200 {
		phase := pollStartPhase(interval)
		require.GreaterOrEqual(t, phase, time.Duration(0))
		require.Less(t, phase, interval)
		seen[phase] = true
	}
	require.Greater(t, len(seen), 1, "the start phase must differ between nodes")
}
