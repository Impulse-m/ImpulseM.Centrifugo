package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// newPendingTestHandler returns a set up handler with the personal channel enabled, which reads the auth
// class claim, and its access revocation store. The node serves real connections.
func newPendingTestHandler(t *testing.T) (*Handler, *accessrevoke.Store, *centrifuge.Node) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	return newAuthClassTestHandler(t, cfg)
}

// newAuthClassTestHandler returns a set up handler with cfg, which reads the auth class claim, and its access
// revocation store. The node serves real connections.
func newAuthClassTestHandler(t *testing.T, cfg config.Config) (*Handler, *accessrevoke.Store, *centrifuge.Node) {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{
		HMACSecretKey:   "secret",
		GenerationClaim: "gen",
		AuthClassClaim:  "auth_class",
	}, cfgContainer)
	require.NoError(t, err)
	store := accessrevoke.NewStore(time.Minute)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetAccessRevoke(store, nil)
	require.NoError(t, h.Setup())
	return h, store, node
}

func TestPendingConnectIsAnonymousToCore(t *testing.T) {
	h, _, _ := newPendingTestHandler(t)
	now := time.Now().Unix()

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        revokeTestUUID,
			"iat":        now,
			"auth_class": "pending_2fa",
		}),
	}, nil, false)
	require.NoError(t, err)
	require.NotNil(t, reply.Credentials)
	require.Empty(t, reply.Credentials.UserID)

	personalChannel := h.cfgContainer.PersonalChannel("42")
	_, hasPersonal := reply.Subscriptions[personalChannel]
	require.False(t, hasPersonal)
	for channel := range reply.Subscriptions {
		require.False(t, strings.Contains(channel, "#"), "user channel %q for a pending connection", channel)
	}
}

func TestPendingConnectionIsNotInUserIndexAndIsRevocableBySession(t *testing.T) {
	tests := []struct {
		name   string
		userID string
	}{
		{name: "with user", userID: "42"},
		{name: "without user", userID: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, store, node := newPendingTestHandler(t)
			now := time.Now().Unix()
			queue := accessrevoke.NewDisconnectQueue(10000, 100)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			go func() { _ = queue.Run(ctx) }()
			revoker := accessrevoke.NewRevoker(store, node.Hub(), queue)

			client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
				"sub":        "42",
				"jti":        revokeTestUUID,
				"iat":        now,
				"auth_class": "pending_2fa",
			}))
			require.Empty(t, client.UserID())
			require.Empty(t, node.Hub().UserConnections("42"))
			require.Len(t, node.Hub().Connections(), 1)
			sessionKey, _ := accessrevoke.Key(revokeTestUUID)
			require.Len(t, store.SessionConnections().Connections(sessionKey), 1)

			require.NoError(t, revoker.RevokeSession(tt.userID, revokeTestUUID, now, 0, nil))
			require.Eventually(t, func() bool {
				return len(node.Hub().Connections()) == 0
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}

func TestPendingConnectionLeavesSessionIndexOnDisconnect(t *testing.T) {
	_, store, node := newPendingTestHandler(t)
	now := time.Now().Unix()

	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub":        "42",
		"jti":        revokeTestUUID,
		"iat":        now,
		"auth_class": "pending_2fa",
	}))
	sessionKey, _ := accessrevoke.Key(revokeTestUUID)
	require.Len(t, store.SessionConnections().Connections(sessionKey), 1)

	client.Disconnect(centrifuge.DisconnectForceNoReconnect)
	require.Eventually(t, func() bool {
		return len(store.SessionConnections().Connections(sessionKey)) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

// TestConnectRevocationAfterTrackDisconnects applies a session revocation between the tracking and the recheck of a
// connect. The session is in the index by then, so the connection is disconnected, and it leaves the index.
func TestConnectRevocationAfterTrackDisconnects(t *testing.T) {
	_, store, node := newPendingTestHandler(t)
	now := time.Now().Unix()
	queue := accessrevoke.NewDisconnectQueue(10000, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = queue.Run(ctx) }()
	revoker := accessrevoke.NewRevoker(store, node.Hub(), queue)
	sessionKey, _ := accessrevoke.Key(revokeTestUUID)

	var trackedAtHook int
	var revokeErr error
	hook := func(rebindableClient) {
		trackedAtHook = len(store.SessionConnections().Connections(sessionKey))
		revokeErr = revoker.RevokeSession("", revokeTestUUID, now, 0, nil)
	}
	testHookConnectTracked.Store(&hook)
	t.Cleanup(func() { testHookConnectTracked.Store(nil) })

	connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub":        "42",
		"jti":        revokeTestUUID,
		"iat":        now - 10,
		"auth_class": "pending_2fa",
	}))
	require.NoError(t, revokeErr)
	require.Equal(t, 1, trackedAtHook)
	require.Eventually(t, func() bool {
		return len(node.Hub().Connections()) == 0
	}, 5*time.Second, 5*time.Millisecond)
	// The index is released by the disconnect handler, which runs after the connection leaves the hub.
	require.Eventually(t, func() bool {
		return len(store.SessionConnections().Connections(sessionKey)) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

func TestPendingRenewalKeepsUnchangedPathAndTracksSession(t *testing.T) {
	h, store, _ := newPendingTestHandler(t)
	now := time.Now().Unix()
	// A pending connection established by a token has its token user in its record.
	client, state := newRebindTestClientWithToken("", "42", "pending_2fa")
	sessions := store.SessionConnections()
	sessions.Track(client, &state.record.track, [16]byte{1})

	nextSessionID := "0a9b8c7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d"
	reply, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        nextSessionID,
			"iat":        now,
			"auth_class": "pending_2fa",
		}),
	}, nil)
	require.NoError(t, err)
	require.True(t, extra.CheckSubs)
	require.NotNil(t, reply.TokenIdentity)
	nextKey, _ := accessrevoke.Key(nextSessionID)
	require.Equal(t, nextKey, reply.TokenIdentity.Session)
	require.Equal(t, []accessrevoke.Connection{client}, sessions.Connections(nextKey))
	require.Empty(t, sessions.Connections([16]byte{1}))
}

// TestConnectRecordHoldsTokenUser checks that a connect keeps the user of its token in the record of the connection,
// whatever its class. A pending connection is anonymous to Core, so the record is what keeps its token user.
func TestConnectRecordHoldsTokenUser(t *testing.T) {
	tests := []struct {
		name    string
		sub     string
		class   string
		wantRec bool
	}{
		{name: "anonymous", class: "anonymous", wantRec: true},
		{name: "authenticated", sub: "42", class: "authenticated", wantRec: false},
		{name: "pending", sub: "42", class: "pending_2fa", wantRec: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, _ := newPendingTestHandler(t)
			claims := map[string]any{
				"jti":        revokeTestUUID,
				"iat":        time.Now().Unix(),
				"auth_class": tt.class,
			}
			if tt.sub != "" {
				claims["sub"] = tt.sub
			}
			reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
				ClientID: "c1",
				Token:    getConnTokenWithClaims(t, claims),
			}, nil, false)
			require.NoError(t, err)
			rec, ok := reply.Storage[storageKeyConnState].(*connState)
			require.Equal(t, tt.wantRec, ok)
			if !ok {
				return
			}
			require.Equal(t, tt.sub, rec.identity.Load().tokenUser)
		})
	}
}

func TestPendingRenewalWithRebindOffKeepsUnchangedPath(t *testing.T) {
	h, _, _ := newPendingTestHandler(t)
	now := time.Now().Unix()
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        revokeTestUUID,
			"iat":        now,
			"auth_class": "pending_2fa",
		}),
	}, nil, false)
	require.NoError(t, err)

	client := newRegistryTestClient("c1", "", nil, reply.Storage)
	client.RebindFunc = func(centrifuge.RebindOptions) error {
		t.Fatal("a pending renewal must not rebind")
		return nil
	}
	_, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        revokeTestOtherUUID,
			"iat":        now,
			"auth_class": "pending_2fa",
		}),
	}, nil)
	require.NoError(t, err)
	require.True(t, extra.CheckSubs)
}

// TestConnectWithoutRevokeOrUserLimitsHoldsNoState checks that a connection of a handler with neither access revocation
// nor user limits gets no connection state, as it has nothing to release.
func TestConnectWithoutRevokeOrUserLimitsHoldsNoState(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{
		HMACSecretKey:   "secret",
		GenerationClaim: "gen",
		AuthClassClaim:  "auth_class",
	}, cfgContainer)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	require.NoError(t, h.Setup())

	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{"sub": "42"}))
	require.Equal(t, "42", client.UserID())
	require.Nil(t, client.ExtState())
}
