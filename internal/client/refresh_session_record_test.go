package client

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestRefreshToSessionIndexesTheConnection checks that a connection with no user holds its record from connect, and is
// indexed by the session of a refresh to an anonymous token with a session. A revocation of the session right after
// the refresh disconnects it. A refresh to a token without a session leaves the index empty.
func TestRefreshToSessionIndexesTheConnection(t *testing.T) {
	tests := []struct {
		name        string
		refreshID   string
		wantIndexed bool
	}{
		{name: "anonymous token with a session", refreshID: revokeTestOtherUUID, wantIndexed: true},
		{name: "anonymous token without a session", refreshID: "", wantIndexed: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
			now := time.Now().Unix()
			queue := accessrevoke.NewDisconnectQueue(10000, 100)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			go func() { _ = queue.Run(ctx) }()
			revoker := accessrevoke.NewRevoker(store, node.Hub(), queue)

			client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
				"iat": now,
			}))
			require.Empty(t, client.UserID())
			require.NotNil(t, connStateOf(client), "the access store gives a connection with no user its record at connect")

			refreshClaims := map[string]any{"iat": now}
			if tt.refreshID != "" {
				refreshClaims["jti"] = tt.refreshID
			}
			data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
				Id:      2,
				Refresh: &protocol.RefreshRequest{Token: getConnTokenWithClaims(t, refreshClaims)},
			})
			require.NoError(t, err)
			require.True(t, centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20))
			require.Len(t, node.Hub().Connections(), 1)
			if !tt.wantIndexed {
				require.Empty(t, store.SessionConnections().Connections([16]byte{}))
				return
			}

			session, _ := accessrevoke.Key(tt.refreshID)
			require.Len(t, store.SessionConnections().Connections(session), 1)
			require.NoError(t, revoker.RevokeSession("", tt.refreshID, now, 0))
			require.Eventually(t, func() bool {
				return len(node.Hub().Connections()) == 0
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}

// TestDefaultConnectAllocatesNothing checks that the connect of a connection with a user, under the defaults, makes no
// record and allocates nothing in the tracking which runs after it, so a session refresh adds no cost to the default path.
func TestDefaultConnectAllocatesNothing(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, config.DefaultConfig())
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub": "42",
		"jti": revokeTestUUID,
		"iat": time.Now().Unix(),
	}))
	require.Nil(t, client.ExtState())

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = h.trackConnect(client)
	})
	require.Zero(t, allocs)
	require.Nil(t, client.ExtState())
}
