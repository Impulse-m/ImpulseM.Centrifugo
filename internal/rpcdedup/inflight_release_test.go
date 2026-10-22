package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInFlightReleasesMapWhenEmpty adds a call and releases it. The map is dropped once it is empty, so an idle
// connection keeps no bucket array, and the next call allocates it again.
func TestInFlightReleasesMapWhenEmpty(t *testing.T) {
	f := NewInFlight()
	release := f.Add(1, func() {})
	release()
	require.Nil(t, f.calls)

	releaseAgain, ok := f.TryAdd(2, func() {})
	require.True(t, ok)
	require.NotNil(t, f.calls)
	releaseAgain()
	require.Nil(t, f.calls)
}
