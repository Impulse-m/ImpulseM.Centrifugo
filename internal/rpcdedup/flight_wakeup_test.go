package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlightNudgeBeforeMarkRemotePrefills(t *testing.T) {
	flight := NewFlight()
	flight.nudge()
	flight.nudge()
	require.False(t, flight.markRemote(true))
	require.Len(t, flight.wake, 1)

	flight.nudge()
	flight.nudge()
	require.Len(t, flight.wake, 1, "nudges coalesce")
}

func TestFlightNudgeAfterMarkRemoteCoalesces(t *testing.T) {
	flight := NewFlight()
	require.False(t, flight.markRemote(true))
	require.Empty(t, flight.wake)

	flight.nudge()
	flight.nudge()
	flight.nudge()
	require.Len(t, flight.wake, 1)
}

func TestFlightWithoutWakeupHasNoChannel(t *testing.T) {
	flight := NewFlight()
	require.False(t, flight.markRemote(false))
	require.Nil(t, flight.wake)

	require.NotPanics(t, flight.nudge)
	require.Nil(t, flight.wake)
}

func TestFlightMarkRemoteReportsCancelBeforeMark(t *testing.T) {
	flight := NewFlight()
	flight.Cancel()
	require.True(t, flight.markRemote(true))
	require.Len(t, flight.wake, 0)
}
