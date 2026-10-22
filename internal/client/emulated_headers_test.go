package client

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestOnClientConnectingKeepsOnlyEmulatedHeaderNames verifies that the connection context keeps only
// the client headers named in emulated_headers, not the whole connect header map.
func TestOnClientConnectingKeepsOnlyEmulatedHeaderNames(t *testing.T) {
	node := tools.NodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{
		UsesEmulatedHeaders: true,
		EmulatedHeaderNames: map[string]struct{}{"x-auth-tok": {}},
	})

	connectProxyHandler := func(ctx context.Context, _ centrifuge.ConnectEvent) (centrifuge.ConnectReply, proxy.ConnectExtra, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "1"}}, proxy.ConnectExtra{}, nil
	}

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Transport: tools.NewTestTransport(),
		Headers:   map[string]string{"User-Agent": "x", "X-Auth-Tok": "y"},
	}, connectProxyHandler, false)
	require.NoError(t, err)

	require.NotNil(t, reply.Context)
	headers, ok := clientcontext.GetEmulatedHeadersFromContext(reply.Context)
	require.True(t, ok)
	require.Equal(t, map[string]string{"X-Auth-Tok": "y"}, headers)
}
