package rpcregistry

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// fakeConn records how many times it was closed.
type fakeConn struct {
	closed atomic.Int32
}

func (c *fakeConn) Close() error {
	c.closed.Add(1)
	return nil
}

// fakeDialer counts dials. When failOn is set, the dial of the target with that name fails.
type fakeDialer struct {
	dials  int
	failOn string
	conns  []*fakeConn
}

func (d *fakeDialer) dial(target *Target) (io.Closer, error) {
	d.dials++
	if target.Config.Name == d.failOn {
		return nil, errors.New("dial refused")
	}
	conn := &fakeConn{}
	d.conns = append(d.conns, conn)
	return conn, nil
}

func newRegistryWithDialer(t *testing.T, dialer *fakeDialer) *Registry {
	t.Helper()
	r := New()
	r.dial = dialer.dial
	return r
}

func reloadWith(t *testing.T, r *Registry, reg configtypes.RPCRegistry) {
	t.Helper()
	apply, err := r.Prepare(reg)
	require.NoError(t, err)
	apply()
}

func TestUnchangedTargetIsReusedWithItsConnection(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reloadWith(t, r, reg)
	first := r.Lookup(testGetUserMethod).Target
	require.Equal(t, 1, dialer.dials)
	require.NotNil(t, first.conn)

	// Change only a method, the target config is the same.
	reg.Methods[1].Timeout = configtypes.Duration(2 * time.Second)
	reloadWith(t, r, reg)

	require.Same(t, first, r.Lookup(testGetUserMethod).Target)
	require.Equal(t, 1, dialer.dials)
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
	require.Empty(t, r.retiredTargets())
}

func TestChangedTargetIsReplacedAndOldRetired(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reloadWith(t, r, reg)
	old := r.Lookup(testGetUserMethod).Target

	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	reloadWith(t, r, reg)

	current := r.Lookup(testGetUserMethod).Target
	require.NotSame(t, old, current)
	require.Equal(t, "dns:///users-new.example:9000", current.Config.Endpoint)
	require.Equal(t, 2, dialer.dials)
	require.Equal(t, []*Target{old}, r.retiredTargets())
	// Closing is not done in this phase, the old connection stays open until the connection code closes it.
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
}

func TestInFlightMethodKeepsItsOldValues(t *testing.T) {
	r := New()
	reg := testRegistry()
	reloadWith(t, r, reg)
	inFlight := r.Lookup(testGetUserMethod)

	reg.Targets[0].Timeout = configtypes.Duration(9 * time.Second)
	reg.Methods[0].GRPCMethod = "/example.v1.UserService/GetUserV2"
	reloadWith(t, r, reg)

	require.Equal(t, 3*time.Second, inFlight.Timeout)
	require.Equal(t, "/example.v1.UserService/GetUser", inFlight.GRPCMethod)
	require.Equal(t, 9*time.Second, r.Lookup(testGetUserMethod).Timeout)
	require.Equal(t, "/example.v1.UserService/GetUserV2", r.Lookup(testGetUserMethod).GRPCMethod)
}

func TestRemovedTargetIsRetired(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reloadWith(t, r, reg)
	old := r.Lookup(testGetUserMethod).Target

	reg.Targets = []configtypes.RPCTarget{{Name: "files", Endpoint: "dns:///files:9000"}}
	reg.Methods = []configtypes.RPCMethod{
		{Method: "files.get", GRPCMethod: "/example.v1.FileService/Get", Target: "files", AuthClass: "anonymous"},
	}
	reloadWith(t, r, reg)

	require.Nil(t, r.Lookup(testGetUserMethod))
	require.Equal(t, []*Target{old}, r.retiredTargets())
}

func TestDialFailureClosesNewConnectionsAndKeepsOldTable(t *testing.T) {
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reg.Targets = append(reg.Targets, configtypes.RPCTarget{Name: "files", Endpoint: "dns:///files:9000"})
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method: "files.get", GRPCMethod: "/example.v1.FileService/Get", Target: "files",
	})
	reloadWith(t, r, reg)
	before := r.Lookup(testGetUserMethod)

	// Change both targets and make the second dial fail. The connection dialed for the first
	// target is closed again, and the old connections stay open.
	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	reg.Targets[1].Endpoint = "dns:///files-new.example:9000"
	dialer.failOn = "files"
	_, err := r.Prepare(reg)
	require.ErrorContains(t, err, "dial refused")

	require.Len(t, dialer.conns, 3)
	require.Equal(t, int32(0), dialer.conns[0].closed.Load())
	require.Equal(t, int32(0), dialer.conns[1].closed.Load())
	require.Equal(t, int32(1), dialer.conns[2].closed.Load())
	require.Same(t, before, r.Lookup(testGetUserMethod))
	require.Empty(t, r.retiredTargets())
}

func TestDrainTimeoutIsUsedForRetiredTargets(t *testing.T) {
	r := New()
	reg := testRegistry()
	reg.DrainTimeout = configtypes.Duration(time.Minute)
	reloadWith(t, r, reg)
	old := r.Lookup(testGetUserMethod).Target

	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	reloadWith(t, r, reg)

	require.Equal(t, []*Target{old}, r.retiredTargets())
	require.True(t, r.retired[0].closeAt.After(time.Now().Add(50*time.Second)))
}
