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
// connection whose token had no session at connect has no record, and its refresh indexes it by a session track, so a
// revocation of that session reaches it through the index. A connection whose token had a session at connect is moved
// to the session of its refresh in the index.
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
			go func() {
				_ = queue.Run(ctx)
			}()
			revoker := accessrevoke.NewRevokerWithMaxTTL(store, node.Hub(), queue, nil)

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
				require.Nil(t, connStateOf(client), "the refresh gives no record")
				require.NotNil(t, sessionTrackOf(client))
				require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(refreshSession))
			} else {
				connectSession, _ := accessrevoke.Key(tt.connectID)
				require.Empty(t, store.SessionConnections().Connections(connectSession))
				require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(refreshSession))
			}
			require.NoError(t, revoker.RevokeSession("", tt.refreshID, now, 0, nil))
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
		// With no session at connect there is no record, so the refresh sets the session track of the connection, which
		// Core refuses for a closed connection. The race of a refresh with a disconnect is in its own test.
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
			if tt.connectID == "" {
				require.Nil(t, client.ExtState(), "a closed connection gets no session track")
			} else {
				connectSession, _ := accessrevoke.Key(tt.connectID)
				require.Empty(t, store.SessionConnections().Connections(connectSession))
			}
		})
	}
}

// TestRefreshRacingDisconnectIndexesNothing runs a refresh to a token with a session and the disconnect of a connection,
// many times. A connection whose token had another session at connect has a record and its disconnect handler from
// connect. A connection with no session at connect has no record: its refresh sets its session track and the disconnect
// handler of the track. Whichever of the two comes first, neither session holds the connection once it is gone.
func TestRefreshRacingDisconnectIndexesNothing(t *testing.T) {
	tests := []struct {
		name       string
		connectID  string
		iterations int
	}{
		{name: "record from connect", connectID: revokeTestUUID, iterations: 50},
		{name: "no record at connect", connectID: "", iterations: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
			now := time.Now().Unix()
			connectSession, _ := accessrevoke.Key(revokeTestUUID)
			refreshSession, _ := accessrevoke.Key(revokeTestOtherUUID)
			refreshToken := getConnTokenWithClaims(t, map[string]any{
				"iat": now,
				"jti": revokeTestOtherUUID,
			})
			connectClaims := map[string]any{"iat": now}
			if tt.connectID != "" {
				connectClaims["jti"] = tt.connectID
			}
			for i := 0; i < tt.iterations; i++ {
				client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, connectClaims))
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _, _ = h.OnRefresh(client, centrifuge.RefreshEvent{Token: refreshToken}, nil)
				}()
				client.Disconnect(centrifuge.DisconnectForceNoReconnect)
				wg.Wait()
				// The hub drops a connection before its disconnect handler runs, so the index is checked until the handler
				// has released the connection, not at once.
				require.Eventually(t, func() bool {
					return len(store.SessionConnections().Connections(connectSession)) == 0 &&
						len(store.SessionConnections().Connections(refreshSession)) == 0
				}, 5*time.Second, 5*time.Millisecond)
			}
		})
	}
}

// refreshWithClaims refreshes the connection with a token of claims, and fails the test when the refresh errs.
func refreshWithClaims(t *testing.T, h *Handler, client *centrifuge.Client, claims map[string]any) centrifuge.RefreshReply {
	t.Helper()
	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{Token: getConnTokenWithClaims(t, claims)}, nil)
	require.NoError(t, err)
	return reply
}

// TestRefreshedSessionMovesAndLeaves refreshes a connection with no record to one session, then to another, then to a
// token without a session. The connection is indexed under the session of its last refresh only, and one session track
// serves the three refreshes.
func TestRefreshedSessionMovesAndLeaves(t *testing.T) {
	h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{"iat": now}))
	firstSession, _ := accessrevoke.Key(revokeTestUUID)
	secondSession, _ := accessrevoke.Key(revokeTestOtherUUID)

	refreshWithClaims(t, h, client, map[string]any{"iat": now, "jti": revokeTestUUID})
	track := sessionTrackOf(client)
	require.NotNil(t, track)
	require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(firstSession))
	require.Empty(t, store.SessionConnections().Connections(secondSession))

	refreshWithClaims(t, h, client, map[string]any{"iat": now, "jti": revokeTestOtherUUID})
	require.Same(t, track, sessionTrackOf(client))
	require.Empty(t, store.SessionConnections().Connections(firstSession))
	require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(secondSession))

	refreshWithClaims(t, h, client, map[string]any{"iat": now})
	require.Same(t, track, sessionTrackOf(client))
	require.Empty(t, store.SessionConnections().Connections(firstSession))
	require.Empty(t, store.SessionConnections().Connections(secondSession))
}

// TestRefreshedSessionRevokedBeforeCoreStoresIdentity revokes the session of a refresh, through a revoker on the same
// store, after the session is tracked and before the check. The check sees the revocation, the refresh expires, and the
// connection goes, whatever identity Core held for it at that moment.
func TestRefreshedSessionRevokedBeforeCoreStoresIdentity(t *testing.T) {
	h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	queue := accessrevoke.NewDisconnectQueue(10000, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = queue.Run(ctx)
	}()
	revoker := accessrevoke.NewRevokerWithMaxTTL(store, node.Hub(), queue, nil)
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{"iat": now}))

	hook := func(_ rebindableClient) {
		require.NoError(t, revoker.RevokeSession("", revokeTestOtherUUID, now, 0, nil))
	}
	testHookRefreshTracked.Store(&hook)
	t.Cleanup(func() {
		testHookRefreshTracked.Store(nil)
	})

	reply := refreshWithClaims(t, h, client, map[string]any{"iat": now, "jti": revokeTestOtherUUID})
	require.True(t, reply.Expired)
	require.Eventually(t, func() bool {
		return len(node.Hub().Connections()) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

// TestRefreshedSessionReleasedOnDisconnect checks that the disconnect of a connection with no record releases the session
// track it got at a refresh: it leaves the session it was refreshed into.
func TestRefreshedSessionReleasedOnDisconnect(t *testing.T) {
	h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{"iat": now}))
	session, _ := accessrevoke.Key(revokeTestUUID)

	refreshWithClaims(t, h, client, map[string]any{"iat": now, "jti": revokeTestUUID})
	require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(session))

	client.Disconnect(centrifuge.DisconnectForceNoReconnect)
	require.Eventually(t, func() bool {
		return len(node.Hub().Connections()) == 0 && len(store.SessionConnections().Connections(session)) == 0
	}, 5*time.Second, 5*time.Millisecond)
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
