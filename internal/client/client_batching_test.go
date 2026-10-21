package client

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestConnectReplyMaxMessagesInFrame checks that the connect reply carries client.max_messages_in_frame to Core.
func TestConnectReplyMaxMessagesInFrame(t *testing.T) {
	tests := []struct {
		name  string
		value int
		want  int
	}{
		{name: "default", value: config.DefaultConfig().Client.MaxMessagesInFrame, want: 16},
		{name: "one message per frame", value: 1, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := tools.NodeWithMemoryEngine()
			defer func() { _ = node.Shutdown(context.Background()) }()

			cfg := config.DefaultConfig()
			cfg.Client.MaxMessagesInFrame = tt.value
			cfgContainer, err := config.NewContainer(cfg)
			require.NoError(t, err)
			verifier := hmacJWTVerifier(t, cfgContainer)
			h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})

			reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
				Token: getConnTokenHS("42", 2525637058),
			}, nil, false)
			require.NoError(t, err)
			require.Equal(t, tt.want, reply.MaxMessagesInFrame)
		})
	}
}
