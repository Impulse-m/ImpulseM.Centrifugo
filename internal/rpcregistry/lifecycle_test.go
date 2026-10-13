package rpcregistry

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestDiscardClosesConnectionsDialedByPrepare(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reloadWith(t, r, testRegistry())
	before := r.Lookup(testGetUserMethod)

	reg := testRegistry()
	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	apply, discard, err := r.PrepareReload(reg)
	require.NoError(t, err)
	require.Len(t, dialer.conns, 2)

	// A reload which fails after Prepare discards it: the new connection is closed and the
	// published table keeps the old target.
	discard()
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
	require.Equal(t, int32(1), dialer.conns[1].closed.Load())
	require.Same(t, before, r.Lookup(testGetUserMethod))

	// Apply after discard does nothing.
	apply()
	require.Same(t, before, r.Lookup(testGetUserMethod))
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
}

func TestDiscardAfterApplyDoesNothing(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reloadWith(t, r, testRegistry())

	reg := testRegistry()
	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	apply, discard, err := r.PrepareReload(reg)
	require.NoError(t, err)
	apply()
	discard()

	require.Len(t, dialer.conns, 2)
	require.Equal(t, int32(0), dialer.conns[1].closed.Load())
	require.Equal(t, "dns:///users-new.example:9000", r.Lookup(testGetUserMethod).Target.Config.Endpoint)
}

func TestRetiredTargetConnectionClosesAfterDrain(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reg.DrainTimeout = configtypes.Duration(300 * time.Millisecond)
	reloadWith(t, r, reg)

	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	reloadWith(t, r, reg)

	// The old connection stays open during the drain, so calls holding the old method finish.
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
	require.Eventually(t, func() bool {
		return dialer.conns[0].closed.Load() == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(0), dialer.conns[1].closed.Load())
}

func TestServiceClosesCurrentTargetsOnStop(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reloadWith(t, r, testRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.Service().Run(ctx)
	}()
	require.Never(t, func() bool {
		return dialer.conns[0].closed.Load() != 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
	require.Equal(t, int32(1), dialer.conns[0].closed.Load())
}

func TestEnabledFollowsConfig(t *testing.T) {
	var nilRegistry *Registry
	require.False(t, nilRegistry.Enabled())
	require.False(t, nilRegistry.UsesEmulatedHeaders())

	r := New()
	require.False(t, r.Enabled())

	reloadWith(t, r, testRegistry())
	require.True(t, r.Enabled())
	require.True(t, r.UsesEmulatedHeaders())

	disabled := testRegistry()
	disabled.Enabled = false
	reloadWith(t, r, disabled)
	require.False(t, r.Enabled())
	require.False(t, r.UsesEmulatedHeaders())
	require.Nil(t, r.Lookup(testGetUserMethod))
}
