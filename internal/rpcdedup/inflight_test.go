package rpcdedup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInFlightCancelReachesOnlyItsCall(t *testing.T) {
	calls := NewInFlight()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := calls.Add(5, cancel)
	calls.Cancel(6)
	require.NoError(t, ctx.Err())
	calls.Cancel(5)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	release()
}

// A release of an older call must not remove a newer call with the same msg_id.
func TestInFlightReleaseKeepsNewerCall(t *testing.T) {
	calls := NewInFlight()
	_, cancelOld := context.WithCancel(context.Background())
	releaseOld := calls.Add(5, cancelOld)
	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	releaseNew := calls.Add(5, cancelNew)

	releaseOld()
	calls.Cancel(5)
	require.ErrorIs(t, newCtx.Err(), context.Canceled)
	releaseNew()
}

func TestFlightAttachAfterCancelRunsCancel(t *testing.T) {
	flight := NewFlight()
	flight.Cancel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flight.Attach(cancel)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
