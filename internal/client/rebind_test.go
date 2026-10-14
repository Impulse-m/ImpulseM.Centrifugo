package client

import (
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const rebindTestClaim = "auth_class"

// rebindTestState is what a fake connection holds and records for a refresh test.
type rebindTestState struct {
	user         string
	rebinds      []centrifuge.RebindOptions
	rebindErr    error
	beforeApply  func(opts centrifuge.RebindOptions)
	channels     map[string]centrifuge.ChannelContext
	subscribed   map[string]centrifuge.SubscribeOptions
	unsubscribed map[string]centrifuge.Unsubscribe
	state        *ratelimit.ConnState
}

// newRebindTestClient returns a fake connection of user with class in its storage. Its rebinds, subscribes and
// unsubscribes are recorded in the state.
func newRebindTestClient(user string, class string) (*tools.TestClientMock, *rebindTestState) {
	state := &rebindTestState{
		user:         user,
		channels:     map[string]centrifuge.ChannelContext{},
		subscribed:   map[string]centrifuge.SubscribeOptions{},
		unsubscribed: map[string]centrifuge.Unsubscribe{},
	}
	client := newRegistryTestClient("c1", user, nil, map[string]any{
		clientstorage.KeyAuthClass: registryAuthClass{class: registryTestClass(class), claim: rebindTestClaim},
	})
	client.UserIDFunc = func() string { return state.user }
	client.ExtStateFunc = func() any {
		if state.state == nil {
			return nil
		}
		return state.state
	}
	client.RebindFunc = func(opts centrifuge.RebindOptions) error {
		state.rebinds = append(state.rebinds, opts)
		if state.rebindErr != nil {
			return state.rebindErr
		}
		if state.beforeApply != nil {
			state.beforeApply(opts)
		}
		state.user = opts.UserID
		return nil
	}
	client.ChannelsFunc = func() map[string]centrifuge.ChannelContext {
		return state.channels
	}
	client.IsSubscribedFunc = func(ch string) bool {
		_, ok := state.channels[ch]
		return ok
	}
	client.SubscribeFunc = func(channel string, opts ...centrifuge.SubscribeOption) error {
		so := centrifuge.SubscribeOptions{}
		for _, opt := range opts {
			opt(&so)
		}
		state.subscribed[channel] = so
		state.channels[channel] = centrifuge.ChannelContext{Source: so.Source}
		return nil
	}
	client.UnsubscribeFunc = func(ch string, unsubscribe ...centrifuge.Unsubscribe) {
		state.unsubscribed[ch] = unsubscribe[0]
		delete(state.channels, ch)
	}
	return client, state
}

// registryTestClass returns the auth class of a class name of the tests.
func registryTestClass(name string) rpcregistry.AuthClass {
	class, ok := rpcregistry.ParseAuthClass(name)
	if !ok {
		return 0
	}
	return class
}

// newRebindTestHandler returns a handler whose config has rebind as given, and whose user limits are attached when
// rateLimited is set. The access revocation store is empty.
func newRebindTestHandler(t *testing.T, rebind configtypes.Rebind, rateLimited bool) (*Handler, *accessrevoke.Store) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Client.Rebind = rebind
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.RateLimit.UserCommand.Enabled = rateLimited
	cfg.Channel.UserBoundary = "#"
	cfg.Channel.WithoutNamespace.SubscribeForAnonymous = true
	cfg.Channel.WithoutNamespace.UserLimitedChannels = true
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	store := accessrevoke.NewStore(time.Minute)
	h.SetAccessRevoke(store, nil)
	return h, store
}

// rebindTestToken returns a connection token of class with user sub, which has session jti. An empty sub is an
// anonymous token.
func rebindTestToken(t *testing.T, sub string, class string, jti string, extra map[string]any) string {
	t.Helper()
	claims := map[string]any{
		"jti":        jti,
		"iat":        time.Now().Unix(),
		"auth_class": class,
	}
	if sub != "" {
		claims["sub"] = sub
	}
	for key, value := range extra {
		claims[key] = value
	}
	return getConnTokenWithClaims(t, claims)
}

func TestRebindFlagOffKeepsDisconnect(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{}, false)
	client, state := newRebindTestClient("", "anonymous")

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestUUID, nil),
	}, nil)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)
	require.Empty(t, state.rebinds)
}

// rebindCase is a transition of a refresh: from the class and user of the connection to the class and user of the
// token of the refresh.
type rebindCase struct {
	name           string
	fromClass      string
	fromUser       string
	toClass        string
	toUser         string
	allowDowngrade bool
	wantRebind     bool
	wantRebindUser string
	wantErr        error
}

func TestRebindTransitionMatrix(t *testing.T) {
	tests := []rebindCase{
		{name: "anonymous to authenticated", fromClass: "anonymous", fromUser: "", toClass: "authenticated", toUser: "42", wantRebind: true, wantRebindUser: "42"},
		{name: "anonymous to bot", fromClass: "anonymous", fromUser: "", toClass: "bot", toUser: "42", wantRebind: true, wantRebindUser: "42"},
		{name: "anonymous to pending keeps the unchanged path", fromClass: "anonymous", fromUser: "", toClass: "pending_2fa", toUser: "42"},
		{name: "anonymous to anonymous keeps the unchanged path", fromClass: "anonymous", fromUser: "", toClass: "anonymous", toUser: ""},
		{name: "pending to authenticated", fromClass: "pending_2fa", fromUser: "", toClass: "authenticated", toUser: "42", wantRebind: true, wantRebindUser: "42"},
		{name: "pending to anonymous keeps the unchanged path", fromClass: "pending_2fa", fromUser: "", toClass: "anonymous", toUser: ""},
		{name: "pending to bot is denied", fromClass: "pending_2fa", fromUser: "", toClass: "bot", toUser: "42", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "pending renewal keeps the unchanged path", fromClass: "pending_2fa", fromUser: "", toClass: "pending_2fa", toUser: "42"},
		{name: "authenticated logout is a disconnect without allow_downgrade", fromClass: "authenticated", fromUser: "42", toClass: "anonymous", toUser: "", wantErr: centrifuge.DisconnectInvalidToken},
		{name: "authenticated logout with allow_downgrade", fromClass: "authenticated", fromUser: "42", toClass: "anonymous", toUser: "", allowDowngrade: true, wantRebind: true, wantRebindUser: ""},
		{name: "bot logout is a disconnect without allow_downgrade", fromClass: "bot", fromUser: "42", toClass: "anonymous", toUser: "", wantErr: centrifuge.DisconnectInvalidToken},
		{name: "bot logout with allow_downgrade", fromClass: "bot", fromUser: "42", toClass: "anonymous", toUser: "", allowDowngrade: true, wantRebind: true, wantRebindUser: ""},
		{name: "authenticated to pending is denied", fromClass: "authenticated", fromUser: "42", toClass: "pending_2fa", toUser: "42", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "authenticated to bot of another user is denied", fromClass: "authenticated", fromUser: "42", toClass: "bot", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "authenticated to another user is denied", fromClass: "authenticated", fromUser: "42", toClass: "authenticated", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "bot to another user is denied", fromClass: "bot", fromUser: "42", toClass: "authenticated", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runRebindCase(t, tt)
		})
	}
}

// runRebindCase runs one refresh of a connection in the state of tt, and checks the reply, the rebind and the class.
func runRebindCase(t *testing.T, tt rebindCase) {
	t.Helper()
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: tt.allowDowngrade}, false)
	client, state := newRebindTestClient(tt.fromUser, tt.fromClass)
	token := rebindTestToken(t, tt.toUser, tt.toClass, revokeTestOtherUUID, nil)

	reply, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{Token: token}, nil)
	if tt.wantErr != nil {
		require.Equal(t, tt.wantErr, err)
	} else {
		require.NoError(t, err)
	}
	if tt.wantRebind {
		require.False(t, extra.CheckSubs, "a rebind does not check token subscriptions")
		require.Len(t, state.rebinds, 1)
		require.Equal(t, tt.wantRebindUser, state.rebinds[0].UserID)
		require.Equal(t, tt.wantRebindUser, state.user)
		require.NotNil(t, reply.TokenIdentity)
	} else {
		require.Empty(t, state.rebinds)
	}
	if tt.wantErr != nil {
		// A refused transition keeps the class of the connection.
		storage, release := client.AcquireStorage()
		stored := storage[clientstorage.KeyAuthClass]
		release(storage)
		require.Equal(t, registryAuthClass{class: registryTestClass(tt.fromClass), claim: rebindTestClaim}, stored)
		require.Equal(t, tt.fromUser, state.user)
	}
}

func TestRebindErrorMapping(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "user limit", err: centrifuge.ErrRebindUserLimit, wantErr: centrifuge.DisconnectConnectionLimit},
		{name: "expired", err: centrifuge.ErrRebindExpired, wantErr: centrifuge.ErrorTokenExpired},
		{name: "closed", err: centrifuge.ErrRebindClosed, wantErr: centrifuge.DisconnectConnectionClosed},
		{name: "other", err: errors.New("other"), wantErr: centrifuge.DisconnectServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
			client, state := newRebindTestClient("", "anonymous")
			state.rebindErr = tt.err

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
			}, nil)
			require.Equal(t, tt.wantErr, err)
			require.Empty(t, state.user)
			require.Empty(t, state.subscribed)
		})
	}
}

func TestRebindDowngradeDropsUserAndTokenChannels(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
	client, state := newRebindTestClient("42", "authenticated")
	state.channels = map[string]centrifuge.ChannelContext{
		"#42":     {Source: subsource.UserPersonal},
		"news":    {Source: subsource.ClientAllowed},
		"room#42": {Source: subsource.ClientAllowed},
		"tok":     {Source: subsource.ConnectionToken},
		"prx":     {Source: subsource.SubscribeProxy},
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, state.user)
	require.Equal(t, map[string]bool{"#42": true, "room#42": true, "tok": true, "prx": true}, unsubscribedSet(state))
	require.Equal(t, centrifuge.UnsubscribeCodeServer, state.unsubscribed["tok"].Code)
	require.Contains(t, state.channels, "news")
}

func unsubscribedSet(state *rebindTestState) map[string]bool {
	out := make(map[string]bool, len(state.unsubscribed))
	for channel := range state.unsubscribed {
		out[channel] = true
	}
	return out
}

func TestRebindUpgradeAddsPersonalAndTokenSubscriptions(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClient("", "anonymous")

	reply, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"tok-sub": map[string]any{}},
		}),
	}, nil)
	require.NoError(t, err)
	require.False(t, extra.CheckSubs)
	require.Equal(t, "42", state.user)
	require.NotNil(t, reply.TokenIdentity)

	personalChannel := h.cfgContainer.PersonalChannel("42")
	require.Contains(t, state.subscribed, personalChannel)
	require.Equal(t, uint8(subsource.UserPersonal), state.subscribed[personalChannel].Source)
	require.Contains(t, state.subscribed, "tok-sub")
}

func TestRebindRateLimitStateFollowsUser(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, true)
	client, state := newRebindTestClient("", "anonymous")
	state.state = &ratelimit.ConnState{}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.True(t, state.state.UserAttached())

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.False(t, state.state.UserAttached())
}

func TestRebindFailureKeepsRateLimitStateOfOldUser(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, true)
	client, state := newRebindTestClient("42", "authenticated")
	state.state = &ratelimit.ConnState{}
	h.userLimiter.Attach(state.state, "42")
	state.rebindErr = centrifuge.ErrRebindUserLimit

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.Equal(t, centrifuge.DisconnectConnectionLimit, err)
	require.True(t, state.state.UserAttached())
	require.Equal(t, "42", state.user)
}

func TestRebindRevocationBetweenCheckAndApplyDisconnects(t *testing.T) {
	now := time.Now().Unix()
	tests := []struct {
		name    string
		revoke  func(store *accessrevoke.Store)
		wantErr error
	}{
		{
			name: "block",
			revoke: func(store *accessrevoke.Store) {
				require.NoError(t, store.BlockUser("42", 0))
			},
			wantErr: accessrevoke.DisconnectUserBlocked,
		},
		{
			name: "session",
			revoke: func(store *accessrevoke.Store) {
				require.NoError(t, store.RevokeSession(revokeTestOtherUUID, now, 0))
			},
			wantErr: centrifuge.DisconnectExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
			client, state := newRebindTestClient("", "anonymous")
			state.beforeApply = func(centrifuge.RebindOptions) {
				tt.revoke(store)
			}

			// The token is issued before the revocation, so the revocation is not a loop guard case.
			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now - 10}),
			}, nil)
			require.Equal(t, tt.wantErr, err)
			require.Len(t, state.rebinds, 1)
			require.Equal(t, 0, store.SessionConnections().Len())
		})
	}
}

func TestRebindUpgradeRealConnectionLeavesSessionIndex(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.Rebind.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	h, store, node := newAuthClassTestHandler(t, cfg)
	now := time.Now().Unix()

	client := connectRevokeTestClient(t, node, rebindTestToken(t, "42", "pending_2fa", revokeTestUUID, nil))
	require.Empty(t, client.UserID())
	require.Equal(t, 1, store.SessionConnections().Len())

	_, extra, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{
			"sub":        "42",
			"jti":        revokeTestOtherUUID,
			"iat":        now,
			"auth_class": "authenticated",
		}),
	}, nil)
	require.NoError(t, err)
	require.False(t, extra.CheckSubs)
	require.Equal(t, "42", client.UserID())
	require.Equal(t, 0, store.SessionConnections().Len())
	require.Len(t, node.Hub().UserConnections("42"), 1)
	require.True(t, client.IsSubscribed(h.cfgContainer.PersonalChannel("42")))

	client.Disconnect(centrifuge.DisconnectForceNoReconnect)
	require.Eventually(t, func() bool {
		return store.SessionConnections().Len() == 0
	}, 5*time.Second, 5*time.Millisecond)
}
