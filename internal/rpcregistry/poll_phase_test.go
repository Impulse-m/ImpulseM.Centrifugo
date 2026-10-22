package rpcregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPollStartPhaseStaysWithinOneInterval verifies that the first check of a node starts within one
// interval, and that the start differs between nodes.
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
