package webpush

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefusedAddressRanges(t *testing.T) {
	testCases := []struct {
		address string
		refused bool
	}{
		// The ranges of PRO.
		{address: "127.0.0.1", refused: true},
		{address: "10.1.2.3", refused: true},
		{address: "172.16.0.1", refused: true},
		{address: "192.168.1.1", refused: true},
		{address: "169.254.1.1", refused: true},
		{address: "0.0.0.0", refused: true},
		{address: "224.0.0.1", refused: true},
		{address: "::1", refused: true},
		{address: "fe80::1", refused: true},
		{address: "fd00::1", refused: true},
		{address: "::", refused: true},
		{address: "ff02::1", refused: true},
		// The ranges added by this provider.
		{address: "100.64.0.1", refused: true},
		{address: "100.127.255.254", refused: true},
		{address: "0.1.2.3", refused: true},
		{address: "198.18.0.1", refused: true},
		{address: "198.19.255.255", refused: true},
		{address: "240.0.0.1", refused: true},
		{address: "fec0::1", refused: true},
		{address: "64:ff9b::1", refused: true},
		{address: "64:ff9b:1::1", refused: true},
		{address: "2002::1", refused: true},
		{address: "2001::1", refused: true},
		{address: "::1.2.3.4", refused: true},
		// The forms that embed an IPv4 address are judged as the IPv4 address.
		{address: "::ffff:127.0.0.1", refused: true},
		{address: "::ffff:10.0.0.1", refused: true},
		{address: "::ffff:100.64.0.1", refused: true},
		// A zone does not change the range.
		{address: "fe80::1%eth0", refused: true},
		// Public addresses on both sides of the ranges are allowed.
		{address: "8.8.8.8", refused: false},
		{address: "100.63.255.255", refused: false},
		{address: "100.128.0.1", refused: false},
		{address: "198.20.0.1", refused: false},
		{address: "2606:4700::1111", refused: false},
		{address: "::ffff:8.8.8.8", refused: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.address, func(t *testing.T) {
			address, err := netip.ParseAddr(testCase.address)
			require.NoError(t, err)

			require.Equal(t, testCase.refused, refusedAddress(address))
		})
	}
}

// TestRefusedAddressRangeBoundaries checks the first and the last address of the private and carrier-grade NAT ranges, and
// the addresses just outside them.
func TestRefusedAddressRangeBoundaries(t *testing.T) {
	testCases := []struct {
		address string
		refused bool
	}{
		{address: "172.15.255.255", refused: false},
		{address: "172.16.0.0", refused: true},
		{address: "172.31.255.255", refused: true},
		{address: "172.32.0.1", refused: false},
		{address: "100.63.255.255", refused: false},
		{address: "100.64.0.0", refused: true},
		{address: "100.127.255.255", refused: true},
		{address: "100.128.0.0", refused: false},
		{address: "fc00::1", refused: true},
		{address: "fdff::1", refused: true},
		{address: "fe00::1", refused: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.address, func(t *testing.T) {
			address, err := netip.ParseAddr(testCase.address)
			require.NoError(t, err)

			require.Equal(t, testCase.refused, refusedAddress(address))
		})
	}
}

func TestInvalidAddressIsRefused(t *testing.T) {
	require.True(t, refusedAddress(netip.Addr{}))
}

func TestRefusedAttemptReadsTheAddressPort(t *testing.T) {
	require.True(t, refusedAttempt("127.0.0.1:443"))
	require.True(t, refusedAttempt("[fe80::1%eth0]:443"))
	require.True(t, refusedAttempt("not an address"))
	require.False(t, refusedAttempt("8.8.8.8:443"))
}

func TestGuardRefusesADialToLoopback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()

	guard := &addressGuard{}
	conn, err := dialContext(guard.Context(context.Background()), "tcp", listener.Addr().String())

	require.Error(t, err)
	require.Nil(t, conn)
	require.True(t, guard.Refused(), "the refusal is recorded on the request")
}

func TestGuardRecordsNothingWithoutARefusal(t *testing.T) {
	guard := &addressGuard{}
	require.False(t, guard.Refused())

	guard.refuse()
	require.True(t, guard.Refused())

	var none *addressGuard
	none.refuse()
}
