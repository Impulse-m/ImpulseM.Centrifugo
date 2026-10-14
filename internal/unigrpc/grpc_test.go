package unigrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/unigrpc/unistream"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// fakeConsumeStream is the server side of a Consume call. Frames are discarded: the test only
// needs the client context created by the stream.
type fakeConsumeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeConsumeStream) Context() context.Context {
	return s.ctx
}

func (s *fakeConsumeStream) Send(*unistream.Push) error {
	return nil
}

func (s *fakeConsumeStream) SendMsg(any) error {
	return nil
}

// connectedClientContext runs Consume with a stream whose peer is peerAddr and returns the context
// of the client which connected.
func connectedClientContext(t *testing.T, clientAddress configtypes.ClientAddress, peerAddr net.Addr) context.Context {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	connected := make(chan context.Context, 1)
	node.OnConnecting(func(ctx context.Context, _ centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		connected <- ctx
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{}}, nil
	})

	streamCtx, cancel := context.WithCancel(peer.NewContext(context.Background(), &peer.Peer{Addr: peerAddr}))
	t.Cleanup(cancel)
	stream := &fakeConsumeStream{ctx: streamCtx}
	service := NewService(node, Config{}, clientAddress)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = service.Consume(&unistream.ConnectRequest{}, stream)
	}()

	var clientCtx context.Context
	select {
	case clientCtx = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not connect")
	}
	cancel()
	<-done
	return clientCtx
}

func TestConsumePeerAddressReachesClientContext(t *testing.T) {
	enabled := configtypes.ClientAddress{Enabled: true}
	clientCtx := connectedClientContext(t, enabled, &net.TCPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4567})
	addr, ok := clientaddr.FromContext(clientCtx)
	require.True(t, ok)
	require.Equal(t, "198.51.100.7", addr.String())
}

func TestConsumePeerAddressIsNotSetWhenDisabled(t *testing.T) {
	clientCtx := connectedClientContext(t, configtypes.ClientAddress{}, &net.TCPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4567})
	_, ok := clientaddr.FromContext(clientCtx)
	require.False(t, ok)
}
