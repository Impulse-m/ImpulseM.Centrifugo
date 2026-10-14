package rpcdedup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInFlightCancelReachesOnlyItsCall(t *testing.T) {
	var calls *InFlight = NewInFlight()
	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var release func() = calls.Add(5, cancel)
	require.True(t, calls.Cancel(5))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	release()

	require.False(t, calls.Cancel(5))
	require.False(t, calls.Cancel(6))
}

// A release of an older call must not remove a newer call with the same msg_id.
func TestInFlightReleaseKeepsNewerCall(t *testing.T) {
	var calls *InFlight = NewInFlight()
	var cancelOld context.CancelFunc
	_, cancelOld = context.WithCancel(context.Background())
	var releaseOld func() = calls.Add(5, cancelOld)
	var newCtx context.Context
	var cancelNew context.CancelFunc
	newCtx, cancelNew = context.WithCancel(context.Background())
	defer cancelNew()
	var releaseNew func() = calls.Add(5, cancelNew)

	releaseOld()
	require.True(t, calls.Cancel(5))
	require.ErrorIs(t, newCtx.Err(), context.Canceled)
	releaseNew()
	require.False(t, calls.Cancel(5))
}

func TestFlightAttachAfterCancelRunsCancel(t *testing.T) {
	var flight *Flight = NewFlight()
	flight.Cancel()
	require.True(t, flight.Cancelled())

	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	flight.Attach(cancel)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
