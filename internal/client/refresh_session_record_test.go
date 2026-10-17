package client

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestRefreshToSessionReachesTheConnection checks how the session of a refresh reaches a connection with no user. A
// connection whose token had no session at connect has no record, so its refresh indexes nothing: a revocation of that
// session reaches it through the sweep which a revocation without a user runs. A connection whose token had a session at
// connect is moved to the session of its refresh in the index.
func TestRefreshToSessionReachesTheConnection(t *testing.T) {
	tests := []struct {
		name      string
		connectID string
		refreshID string
	}{
		{name: "no session at connect, session at refresh", connectID: "", refreshID: revokeTestOtherUUID},
		{name: "no session at connect or refresh", connectID: "", refreshID: ""},
		{name: "session at connect, another at refresh", connectID: revokeTestUUID, refreshID: revokeTestOtherUUID},
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

			connectClaims := map[string]any{"iat": now}
			if tt.connectID != "" {
				connectClaims["jti"] = tt.connectID
			}
			client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, connectClaims))
			require.Empty(t, client.UserID())
			if tt.connectID == "" {
				require.Nil(t, connStateOf(client), "a connection with no session at connect has no record")
			} else {
				connectSession, _ := accessrevoke.Key(tt.connectID)
				require.Len(t, store.SessionConnections().Connections(connectSession), 1)
			}

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

			if tt.refreshID == "" {
				// The refresh token has no jti, so no session is derived from it. The connection must be in none of the
				// sessions this test uses, which is what a bug indexing it under another claim would break.
				for _, id := range []string{revokeTestUUID, revokeTestOtherUUID} {
					session, _ := accessrevoke.Key(id)
					require.Empty(t, store.SessionConnections().Connections(session))
				}
				return
			}

			refreshSession, _ := accessrevoke.Key(tt.refreshID)
			if tt.connectID == "" {
				require.Equal(t, refreshSession, client.TokenIdentity().Session, "Core keeps the identity of the refresh")
				require.Empty(t, store.SessionConnections().Connections(refreshSession))
			} else {
				connectSession, _ := accessrevoke.Key(tt.connectID)
				require.Empty(t, store.SessionConnections().Connections(connectSession))
				require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(refreshSession))
			}
			require.NoError(t, revoker.RevokeSession("", tt.refreshID, now, 0))
			require.Eventually(t, func() bool {
				return len(node.Hub().Connections()) == 0
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}

// TestClosedConnectionIsNotIndexedByRefresh checks that a connection with no user which closed before its refresh to a
// token with a session is not indexed by that session. Its disconnect released what it held, and a refresh of a closed
// connection tracks nothing. A connection with a session at connect has its record, and the record of a closed one stays
// out of the index as well.
func TestClosedConnectionIsNotIndexedByRefresh(t *testing.T) {
	tests := []struct {
		name      string
		connectID string
	}{
		{name: "no session at connect", connectID: ""},
		{name: "session at connect", connectID: revokeTestUUID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
			now := time.Now().Unix()
			connectClaims := map[string]any{"iat": now}
			if tt.connectID != "" {
				connectClaims["jti"] = tt.connectID
			}
			client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, connectClaims))
			client.Disconnect(centrifuge.DisconnectForceNoReconnect)
			require.Eventually(t, func() bool {
				return len(node.Hub().Connections()) == 0
			}, 5*time.Second, 5*time.Millisecond)
			if tt.connectID == "" {
				require.Nil(t, connStateOf(client))
			} else {
				// The hub drops a connection before its disconnect handler runs, so the test waits for the handler to mark
				// the record closed. Only then is the refresh a refresh of a closed connection.
				rec := connStateOf(client)
				require.NotNil(t, rec)
				require.Eventually(t, rec.track.Closed, 5*time.Second, 5*time.Millisecond)
			}

			refreshSession, _ := accessrevoke.Key(revokeTestOtherUUID)
			// The refresh of the closed connection may fail: only the index is checked.
			_, _, _ = h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: getConnTokenWithClaims(t, map[string]any{
					"iat": now,
					"jti": revokeTestOtherUUID,
				}),
			}, nil)
			require.Empty(t, store.SessionConnections().Connections(refreshSession))
			if tt.connectID != "" {
				connectSession, _ := accessrevoke.Key(tt.connectID)
				require.Empty(t, store.SessionConnections().Connections(connectSession))
			}
		})
	}
}

// TestRefreshRacingDisconnectIndexesNothing runs a refresh to a token with a session and the disconnect of a connection
// whose token has another session at connect, many times. Such a connection has a record and its disconnect handler from
// connect, so it is the only kind which can be in the index. Whichever of the two comes first, neither session holds the
// connection once it is gone.
func TestRefreshRacingDisconnectIndexesNothing(t *testing.T) {
	h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	connectSession, _ := accessrevoke.Key(revokeTestUUID)
	refreshSession, _ := accessrevoke.Key(revokeTestOtherUUID)
	refreshToken := getConnTokenWithClaims(t, map[string]any{
		"iat": now,
		"jti": revokeTestOtherUUID,
	})
	for i := 0; i < 50; i++ {
		client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
			"iat": now,
			"jti": revokeTestUUID,
		}))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = h.OnRefresh(client, centrifuge.RefreshEvent{Token: refreshToken}, nil)
		}()
		client.Disconnect(centrifuge.DisconnectForceNoReconnect)
		wg.Wait()
		// The hub drops a connection before its disconnect handler runs, so the index is checked until the handler has
		// released the connection, not at once.
		require.Eventually(t, func() bool {
			return len(store.SessionConnections().Connections(connectSession)) == 0 &&
				len(store.SessionConnections().Connections(refreshSession)) == 0
		}, 5*time.Second, 5*time.Millisecond)
	}
}

// TestDefaultConnectAllocatesNothing checks that the connect of a connection, under the defaults, makes no record and
// allocates nothing in the tracking which runs after it, so the default path adds no cost. Both a connection with a user
// and an anonymous connection without a session are checked.
func TestDefaultConnectAllocatesNothing(t *testing.T) {
	now := time.Now().Unix()
	tests := []struct {
		name   string
		claims map[string]any
	}{
		{name: "user token with a session", claims: map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now}},
		{name: "anonymous token without a session", claims: map[string]any{"iat": now}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, node := newAuthClassTestHandler(t, config.DefaultConfig())
			client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, tt.claims))
			require.Nil(t, client.ExtState())

			allocs := testing.AllocsPerRun(100, func() {
				_, _ = h.trackConnect(client)
			})
			require.Zero(t, allocs)
			require.Nil(t, client.ExtState())
		})
	}
}
