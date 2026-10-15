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

// TestRefreshWithoutExpiryIsRevocableBySession refreshes a connection with a token which has no expiry to a new session,
// through the protocol of Core. Core keeps the identity of a connection only for a token with an expiry, so the
// revocation of the new session disconnects the connection through the identity the server keeps for it.
func TestRefreshWithoutExpiryIsRevocableBySession(t *testing.T) {
	_, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	queue := accessrevoke.NewDisconnectQueue(10000, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = queue.Run(ctx) }()
	revoker := accessrevoke.NewRevoker(store, node.Hub(), queue)

	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub": "42",
		"jti": revokeTestUUID,
		"iat": now,
	}))
	require.Equal(t, "42", client.UserID())

	refreshToken := getConnTokenWithClaims(t, map[string]any{
		"sub": "42",
		"jti": revokeTestOtherUUID,
		"iat": now,
	})
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
		Id:      2,
		Refresh: &protocol.RefreshRequest{Token: refreshToken},
	})
	require.NoError(t, err)
	require.True(t, centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20))
	require.Len(t, node.Hub().Connections(), 1)
	userID, identity := accessrevoke.IdentityOf(client)
	require.Equal(t, "42", userID)
	require.Equal(t, accessrevoke.Identity(revokeTestOtherUUID, 0, now), identity)

	require.NoError(t, revoker.RevokeSession("42", revokeTestOtherUUID, now, 0))
	require.Eventually(t, func() bool {
		return len(node.Hub().Connections()) == 0
	}, 5*time.Second, 5*time.Millisecond)
}
