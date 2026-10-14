package clientaddr

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

func TestMiddlewareStoresResolvedAddress(t *testing.T) {
	r := mustResolver(t, enabledConfig("10.0.0.0/8"))
	var got netip.Addr
	var ok bool
	h := Middleware(r)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		got, ok = FromContext(req.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.9:1"
	req.Header.Set("X-Forwarded-For", "2.2.2.2")
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("2.2.2.2"), got)
}

func TestMiddlewareUnknownAddressStoresNothing(t *testing.T) {
	r := mustResolver(t, enabledConfig())
	var ok bool
	h := Middleware(r)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, ok = FromContext(req.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "raw-addr"
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.False(t, ok)
}

func TestThrottleKeyAndLogAddrPreferResolved(t *testing.T) {
	// Nothing is trusted: the resolved address is the socket peer, the legacy rule would have used the header.
	r := mustResolver(t, enabledConfig())
	var throttleKey, logAddr string
	h := Middleware(r)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		throttleKey = ThrottleKey(req)
		logAddr = LogAddr(req)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.9:1"
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	req.Header.Set("X-Real-IP", "1.1.1.1")
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.Equal(t, "10.0.0.9", throttleKey)
	require.Equal(t, "10.0.0.9", logAddr)
}

func TestThrottleKeyAndLogAddrFallBackToLegacy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.9:1"
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	require.Equal(t, "1.1.1.1", ThrottleKey(req))
	require.Equal(t, "1.1.1.1", LogAddr(req))
}

func TestWithPeer(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("::ffff:2.2.2.2"), Port: 4567},
	})
	addr, ok := FromContext(WithPeer(ctx))
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("2.2.2.2"), addr)

	addr, ok = FromContext(WithPeer(peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443},
	})))
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("2001:db8::1"), addr)

	// No peer, or a peer without an address, leaves the context alone.
	_, ok = FromContext(WithPeer(context.Background()))
	require.False(t, ok)
	_, ok = FromContext(WithPeer(peer.NewContext(context.Background(), &peer.Peer{})))
	require.False(t, ok)
}
