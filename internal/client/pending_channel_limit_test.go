package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestPendingConnectionIsLimitedByChannelLimit checks that a pending connection is subject to client.channel_limit, as any
// other connection is. The channels of its token are subscribed at connect, and a connect over the limit subscribes none.
// The limit is the one of the node, which Core applies to every class of connection.
func TestPendingConnectionIsLimitedByChannelLimit(t *testing.T) {
	node, err := centrifuge.New(centrifuge.Config{ClientChannelLimit: 1})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	_, _, _ = newAuthClassTestHandlerOnNode(t, config.DefaultConfig(), node)

	tests := []struct {
		name          string
		subs          map[string]any
		wantConnected bool
		wantChannels  int
	}{
		{
			name:          "pending at the channel limit",
			subs:          map[string]any{"tok-a": map[string]any{}},
			wantConnected: true,
			wantChannels:  1,
		},
		{
			name:          "pending over the channel limit",
			subs:          map[string]any{"tok-a": map[string]any{}, "tok-b": map[string]any{}},
			wantConnected: false,
			wantChannels:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, connected := connectTestClientFrame(t, node, pendingChannelToken(t, tt.subs))
			require.Equal(t, tt.wantConnected, connected)
			require.Len(t, client.Channels(), tt.wantChannels)
		})
	}
}

// pendingChannelToken returns a connection token of class pending_2fa with user 42 and the subs given.
func pendingChannelToken(t *testing.T, subs map[string]any) string {
	t.Helper()
	return getConnTokenWithClaims(t, map[string]any{
		"sub":        "42",
		"jti":        revokeTestUUID,
		"iat":        time.Now().Unix(),
		"auth_class": "pending_2fa",
		"subs":       subs,
	})
}
