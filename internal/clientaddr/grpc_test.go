package clientaddr

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// fakeServerStream only provides a context: the interceptor never calls the other methods.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context {
	return s.ctx
}

func TestStreamInterceptorSetsPeerAddress(t *testing.T) {
	streamCtx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4567},
	})
	stream := &fakeServerStream{ctx: streamCtx}

	var gotSrv any
	var gotCtx context.Context
	handler := func(srv any, ss grpc.ServerStream) error {
		gotSrv = srv
		gotCtx = ss.Context()
		return nil
	}
	err := StreamInterceptor("service", stream, nil, handler)
	require.NoError(t, err)
	require.Equal(t, "service", gotSrv)

	addr, ok := FromContext(gotCtx)
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("198.51.100.7"), addr)
}

func TestStreamInterceptorWithoutPeerLeavesNoAddress(t *testing.T) {
	stream := &fakeServerStream{ctx: context.Background()}
	var gotCtx context.Context
	err := StreamInterceptor(nil, stream, nil, func(_ any, ss grpc.ServerStream) error {
		gotCtx = ss.Context()
		return nil
	})
	require.NoError(t, err)
	_, ok := FromContext(gotCtx)
	require.False(t, ok)
}
