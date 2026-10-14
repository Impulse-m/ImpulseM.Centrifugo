package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindTokenSubscriptionsFollowTheNewIdentity checks that a rebind to a pending or an anonymous identity
// subscribes the channels of its token, as a connect does, and that a connection without a user gets no personal
// channel.
func TestRebindTokenSubscriptionsFollowTheNewIdentity(t *testing.T) {
	tests := []struct {
		name          string
		fromClass     string
		fromTokenUser string
		toClass       string
		toUser        string
	}{
		{name: "anonymous to pending", fromClass: "anonymous", toClass: "pending_2fa", toUser: "42"},
		{name: "pending to anonymous", fromClass: "pending_2fa", fromTokenUser: "42", toClass: "anonymous", toUser: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
			client, state := newRebindTestClientWithToken("", tt.fromTokenUser, tt.fromClass)

			_, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, tt.toUser, tt.toClass, revokeTestOtherUUID, map[string]any{
					"subs": map[string]any{"tok-sub": map[string]any{}},
				}),
			}, nil)
			require.NoError(t, err)
			require.False(t, extra.CheckSubs)
			require.Empty(t, state.user)
			require.Contains(t, state.subscribed, "tok-sub")
			require.NotContains(t, state.subscribed, h.cfgContainer.PersonalChannel("42"))
		})
	}
}

// TestRebindStoresNewIdentityBeforeCoreUser checks that the class and the token user of the connection are the new
// ones when the Core user changes, so a registry call which overlaps the rebind does not see the old class.
func TestRebindStoresNewIdentityBeforeCoreUser(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
	client, state := newRebindTestClientWithToken("42", "42", "authenticated")
	var storedClass rpcregistry.AuthClass
	var storedTokenUser string
	state.beforeApply = func(centrifuge.RebindOptions) {
		storage, release := client.AcquireStorage()
		defer release(storage)
		stored, _ := storage[clientstorage.KeyAuthClass].(registryAuthClass)
		storedClass = stored.class
		storedTokenUser, _ = storage[storageKeyTokenUser].(string)
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, rpcregistry.AuthClassAnonymous, storedClass)
	require.Empty(t, storedTokenUser)
	require.Empty(t, state.user)
}

// connIsClosed reports whether the handler state of the connection is marked closed.
func connIsClosed(c rebindableClient) bool {
	st := connStateOf(c)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

// closeInRebindHook closes the connection from the rebind hook, and returns once its disconnect has released it.
func closeInRebindHook(t *testing.T, client *centrifuge.Client) {
	t.Helper()
	hook := func(c rebindableClient) {
		client.Disconnect(centrifuge.DisconnectForceNoReconnect)
		require.Eventually(t, func() bool { return connIsClosed(c) }, 5*time.Second, time.Millisecond)
	}
	testHookRebound.Store(&hook)
	t.Cleanup(func() { testHookRebound.Store(nil) })
}

// TestRebindCloseBeforeTrackLeavesNoSession closes a connection between its rebind to pending_2fa and the tracking of
// the session of the new token. The close has released the connection, so no session is left in the index.
func TestRebindCloseBeforeTrackLeavesNoSession(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.Rebind.Enabled = true
	h, store, node := newAuthClassTestHandler(t, cfg)
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "", "anonymous", revokeTestUUID, nil))
	closeInRebindHook(t, client)

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "pending_2fa", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	nextKey, _ := accessrevoke.Key(revokeTestOtherUUID)
	require.Eventually(t, func() bool {
		return len(store.SessionConnections().Connections(nextKey)) == 0
	}, 5*time.Second, 5*time.Millisecond)
	require.Empty(t, store.SessionConnections().Connections(nextKey))
}

// TestRebindCloseBeforeSyncReleasesUserState closes a connection between its rebind to a user and the sync of its
// session. The rate limit state of the user is attached before the rebind, and the close detaches it, so the user
// budget is not held by the closed connection.
func TestRebindCloseBeforeSyncReleasesUserState(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.Rebind.Enabled = true
	cfg.Client.RateLimit.UserCommand = rebindBudgetUserCommand()
	h, _, node := newAuthClassTestHandler(t, cfg)
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "", "anonymous", revokeTestUUID, nil))
	st := rateLimitStateOf(client)
	require.NotNil(t, st)

	hook := func(c rebindableClient) {
		require.True(t, budgetAllowed(h, st), "the connection draws on the budget of 42 once it is rebound")
		client.Disconnect(centrifuge.DisconnectForceNoReconnect)
		require.Eventually(t, func() bool { return connIsClosed(c) }, 5*time.Second, time.Millisecond)
	}
	testHookRebound.Store(&hook)
	t.Cleanup(func() { testHookRebound.Store(nil) })

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.True(t, budgetAllowed(h, st), "the closed connection no longer draws on the budget of 42")
}

// TestRefreshRevokedAfterTrackExpiresConnection revokes the session of a refresh after the session is tracked. The
// check which follows the tracking sees the revocation, so the refresh expires the connection.
func TestRefreshRevokedAfterTrackExpiresConnection(t *testing.T) {
	h, store, node := newAuthClassTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub":        "42",
		"jti":        revokeTestUUID,
		"iat":        now - 10,
		"auth_class": "pending_2fa",
	}))
	require.Empty(t, client.UserID())

	hook := func(rebindableClient) {
		require.NoError(t, store.RevokeSession(revokeTestOtherUUID, now, 0))
	}
	testHookRefreshTracked.Store(&hook)
	t.Cleanup(func() { testHookRefreshTracked.Store(nil) })

	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        revokeTestOtherUUID,
			"iat":        now - 10,
			"auth_class": "pending_2fa",
		}),
	}, nil)
	require.NoError(t, err)
	require.True(t, reply.Expired)
}

// TestRefreshWhileAnotherIsInProgressIsRejected runs a second refresh of a connection from the rebind of the first one.
// The second is rejected, and the connection keeps the identity of the first. A refresh after both is answered.
func TestRefreshWhileAnotherIsInProgressIsRejected(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClient("", "anonymous")
	var secondErr error
	hook := func(c rebindableClient) {
		_, _, secondErr = h.OnRefresh(c, centrifuge.RefreshEvent{
			Token: rebindTestToken(t, "43", "authenticated", revokeTestUUID, nil),
		}, nil)
	}
	testHookRebound.Store(&hook)
	t.Cleanup(func() { testHookRebound.Store(nil) })

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, centrifuge.ErrorTooManyRequests, secondErr)
	require.Equal(t, "42", state.user)
	require.Len(t, state.rebinds, 1)

	testHookRebound.Store(nil)
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
}

// TestRebindDowngradeDropsUserChannelsBeforeTheRebind checks that the personal channel of the user is gone when the
// rebind to the anonymous identity runs.
func TestRebindDowngradeDropsUserChannelsBeforeTheRebind(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
	client, state := newRebindTestClient("42", "authenticated")
	state.channels = map[string]centrifuge.ChannelContext{
		"#42": {Source: subsource.UserPersonal},
	}
	var personalAtRebind bool
	state.beforeApply = func(centrifuge.RebindOptions) {
		_, personalAtRebind = state.channels["#42"]
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Len(t, state.rebinds, 1)
	require.False(t, personalAtRebind)
	require.Empty(t, state.user)
}
