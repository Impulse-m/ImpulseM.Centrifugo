package client

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	record       *connState
}

// newRebindTestClient returns a fake connection of user with class in its record, whose token user is user. Its
// rebinds, subscribes and unsubscribes are recorded in the state.
func newRebindTestClient(user string, class string) (*rebindClientMock, *rebindTestState) {
	return newRebindTestClientWithToken(user, user, class)
}

// newRebindTestRecord returns the record of a connection which holds tokenUser and class, as the connect of a token of
// class leaves it. A connection with no class is one made through a connect proxy, which has no token.
func newRebindTestRecord(tokenUser string, class string) *connState {
	rec := &connState{}
	origin := originProxy
	var authClass rpcregistry.AuthClass
	if class != "" {
		origin = originToken
		authClass = registryTestClass(class)
	}
	rec.identity.Store(nextIdentity(nil, origin, authClass, rebindTestClaim, "", jwtverify.ConnectToken{UserID: tokenUser}))
	return rec
}

// newRebindTestClientWithToken returns a fake connection of Core user, with the record of a connection whose token user
// and class are the ones given.
func newRebindTestClientWithToken(user string, tokenUser string, class string) (*rebindClientMock, *rebindTestState) {
	state := &rebindTestState{
		user:         user,
		channels:     map[string]centrifuge.ChannelContext{},
		subscribed:   map[string]centrifuge.SubscribeOptions{},
		unsubscribed: map[string]centrifuge.Unsubscribe{},
		record:       newRebindTestRecord(tokenUser, class),
	}
	client := newRegistryTestClient("c1", user, nil, nil)
	client.UserIDFunc = func() string { return state.user }
	client.ExtStateFunc = func() any { return state.record }
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
	return newRebindTestHandlerWithUserCommand(t, rebind, configtypes.RateLimitUserCommand{Enabled: rateLimited})
}

// newRebindTestHandlerWithUserCommand is newRebindTestHandler with the user command rate limits of userCommand.
func newRebindTestHandlerWithUserCommand(
	t *testing.T,
	rebind configtypes.Rebind,
	userCommand configtypes.RateLimitUserCommand,
) (*Handler, *accessrevoke.Store) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Client.Rebind = rebind
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.RateLimit.UserCommand = userCommand
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

// rebindCase is a transition of a refresh: from the class, Core user and token user of the connection to the class
// and user of the token of the refresh. An empty fromTokenUser is the Core user, as for a connection with no pending
// class.
type rebindCase struct {
	name           string
	fromClass      string
	fromUser       string
	fromTokenUser  string
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
		{name: "anonymous to pending of a user is a rebind with the Core user kept", fromClass: "anonymous", fromUser: "", toClass: "pending_2fa", toUser: "42", wantRebind: true, wantRebindUser: ""},
		{name: "anonymous to anonymous keeps the unchanged path", fromClass: "anonymous", fromUser: "", toClass: "anonymous", toUser: ""},
		{name: "pending to authenticated of the same user", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "authenticated", toUser: "42", wantRebind: true, wantRebindUser: "42"},
		{name: "pending to anonymous", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "anonymous", toUser: "", wantRebind: true, wantRebindUser: ""},
		{name: "pending to bot is denied", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "bot", toUser: "42", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "pending renewal keeps the unchanged path", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "pending_2fa", toUser: "42"},
		{name: "pending of one user to pending of another is denied", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "pending_2fa", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "pending of one user to authenticated of another is denied", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "authenticated", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "pending of one user to bot of another is denied", fromClass: "pending_2fa", fromUser: "", fromTokenUser: "42", toClass: "bot", toUser: "43", wantErr: centrifuge.ErrorPermissionDenied},
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
	tokenUser := tt.fromTokenUser
	if tokenUser == "" {
		tokenUser = tt.fromUser
	}
	client, state := newRebindTestClientWithToken(tt.fromUser, tokenUser, tt.fromClass)
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
		require.Equal(t, registryTestClass(tt.fromClass), state.record.identity.Load().class)
		require.Equal(t, tt.fromUser, state.user)
	}
}

// TestRebindConnectProxyUserMayNotRebind checks a connection made through a connect proxy: it has a user but no token
// class, so any change of its identity is refused with a permission error, and its renewal of the same user is not.
func TestRebindConnectProxyUserMayNotRebind(t *testing.T) {
	tests := []struct {
		name           string
		toUser         string
		toClass        string
		allowDowngrade bool
		wantErr        error
	}{
		{name: "to another user", toUser: "43", toClass: "authenticated", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "to anonymous", toUser: "", toClass: "anonymous", allowDowngrade: true, wantErr: centrifuge.ErrorPermissionDenied},
		{name: "to anonymous without allow_downgrade", toUser: "", toClass: "anonymous", wantErr: centrifuge.ErrorPermissionDenied},
		{name: "renewal of the same user", toUser: "42", toClass: "authenticated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: tt.allowDowngrade}, false)
			client, state := newRebindTestClient("42", "")

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, tt.toUser, tt.toClass, revokeTestOtherUUID, nil),
			}, nil)
			require.Equal(t, tt.wantErr, err)
			require.Empty(t, state.rebinds)
			require.Equal(t, "42", state.user)
		})
	}
}

// TestRebindFlagOffRoutesAnyTokenUserChangeToDisconnect checks that with rebind off, a change of the token user
// disconnects as upstream does, even when Core sees no user for the connection.
func TestRebindFlagOffRoutesAnyTokenUserChangeToDisconnect(t *testing.T) {
	tests := []struct {
		name          string
		fromUser      string
		fromTokenUser string
		fromClass     string
		toClass       string
		toUser        string
		wantErr       error
	}{
		{name: "anonymous to pending of a user", fromClass: "anonymous", toClass: "pending_2fa", toUser: "42", wantErr: centrifuge.DisconnectInvalidToken},
		{name: "pending of one user to pending of another", fromClass: "pending_2fa", fromTokenUser: "42", toClass: "pending_2fa", toUser: "43", wantErr: centrifuge.DisconnectInvalidToken},
		{name: "pending renewal of the same user keeps the unchanged path", fromClass: "pending_2fa", fromTokenUser: "42", toClass: "pending_2fa", toUser: "42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{}, false)
			tokenUser := tt.fromTokenUser
			if tokenUser == "" {
				tokenUser = tt.fromUser
			}
			client, state := newRebindTestClientWithToken(tt.fromUser, tokenUser, tt.fromClass)

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, tt.toUser, tt.toClass, revokeTestOtherUUID, nil),
			}, nil)
			require.Equal(t, tt.wantErr, err)
			require.Empty(t, state.rebinds)
		})
	}
}

func TestRebindErrorMapping(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "user limit", err: centrifuge.ErrRebindUserLimit, wantErr: centrifuge.ErrorTooManyRequests},
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

// TestRebindPendingDowngradeToAnonymousDropsTokenChannels checks a pending connection, which Core sees as anonymous,
// refreshing to an anonymous token: its token and personal channels go, and its anonymous client channel stays.
func TestRebindPendingDowngradeToAnonymousDropsTokenChannels(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClientWithToken("", "42", "pending_2fa")
	state.channels = map[string]centrifuge.ChannelContext{
		"#42":  {Source: subsource.UserPersonal},
		"news": {Source: subsource.ClientAllowed},
		"tok":  {Source: subsource.ConnectionToken},
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, state.user)
	require.Equal(t, map[string]bool{"#42": true, "tok": true}, unsubscribedSet(state))
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

// rebindBudgetUserCommand returns user command limits with one unsubscribe per minute, a budget a test can spend.
func rebindBudgetUserCommand() configtypes.RateLimitUserCommand {
	return configtypes.RateLimitUserCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Unsubscribe: rateLimitChannelOf(time.Minute, 1),
		},
	}
}

// budgetAllowed takes one unsubscribe from the user budget of the connection state. It is allowed when the state
// has no user, and when the user budget has a command left.
func budgetAllowed(h *Handler, st *ratelimit.ConnState) bool {
	return h.userLimiter.CommandAllowed(st, time.Now(), ratelimit.OperationUnsubscribe, "")
}

// TestRebindRateLimitStateFollowsUser checks that the rate limit state of a connection draws on the budget of its
// user: after an upgrade the budget of the new user is spent by the connection, and after a downgrade it is not.
// TestRebindPendingUnderOtherClaimIsRefused checks that a pending connection, which Core sees with no user, keeps the claim
// check of a connection with a user: its class was derived under another claim, so an upgrade is refused.
func TestRebindPendingUnderOtherClaimIsRefused(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClientWithToken("", "42", "pending_2fa")
	state.record.identity.Store(nextIdentity(nil, originToken, registryTestClass("pending_2fa"), "other_claim", "",
		jwtverify.ConnectToken{UserID: "42"}))
	token := rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil)

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{Token: token}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
	require.Empty(t, state.rebinds)
}

func TestRebindRateLimitStateFollowsUser(t *testing.T) {
	h, _ := newRebindTestHandlerWithUserCommand(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, rebindBudgetUserCommand())
	client, state := newRebindTestClient("", "anonymous")
	st := &ratelimit.ConnState{}
	state.record.limits = st

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.True(t, budgetAllowed(h, st))
	probe := &ratelimit.ConnState{}
	h.userLimiter.Attach(probe, "42")
	require.False(t, budgetAllowed(h, probe), "the budget of 42 is shared with the connection")

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.True(t, budgetAllowed(h, st), "after a downgrade the connection is not limited by the budget of 42")
}

// TestRebindFailureKeepsRateLimitStateOfOldUser checks that a rebind which fails leaves the rate limit state on the
// old user.
func TestRebindFailureKeepsRateLimitStateOfOldUser(t *testing.T) {
	h, _ := newRebindTestHandlerWithUserCommand(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, rebindBudgetUserCommand())
	client, state := newRebindTestClient("42", "authenticated")
	st := &ratelimit.ConnState{}
	state.record.limits = st
	h.userLimiter.Attach(st, "42")
	state.rebindErr = centrifuge.ErrRebindUserLimit

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	// A refusal by the user limit is an error reply, so the connection stays with the identity it had.
	require.Equal(t, centrifuge.ErrorTooManyRequests, err)
	require.Equal(t, "42", state.user)
	// The identity stored before the rebind is restored with the user.
	id := state.record.identity.Load()
	require.Equal(t, registryTestClass("authenticated"), id.class)
	require.Equal(t, "42", id.tokenUser)
	require.True(t, budgetAllowed(h, st))
	probe := &ratelimit.ConnState{}
	h.userLimiter.Attach(probe, "42")
	require.False(t, budgetAllowed(h, probe), "the connection still draws on the budget of 42")
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
				require.NoError(t, store.RevokeSession(revokeTestOtherUUID, now, 0, nil))
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
			sessionKey, _ := accessrevoke.Key(revokeTestOtherUUID)
			require.Empty(t, store.SessionConnections().Connections(sessionKey))
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
	pendingKey, _ := accessrevoke.Key(revokeTestUUID)
	require.Len(t, store.SessionConnections().Connections(pendingKey), 1)

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
	require.Empty(t, store.SessionConnections().Connections(pendingKey))
	require.Len(t, node.Hub().UserConnections("42"), 1)
	require.True(t, client.IsSubscribed(h.cfgContainer.PersonalChannel("42")))

	client.Disconnect(centrifuge.DisconnectForceNoReconnect)
	require.Eventually(t, func() bool {
		return len(store.SessionConnections().Connections(pendingKey)) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

// TestAnonymousConnectExchangeAndRefreshUpgrades checks the flow of an SDK which obtains its connection token over the
// connection: it connects with no token, calls the exchange method, an anonymous method whose call carries no token, and
// refreshes with the access token the exchange returned, which upgrades the connection to the user of the token.
func TestAnonymousConnectExchangeAndRefreshUpgrades(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.AllowAnonymousConnectWithoutToken = true
	cfg.Client.Rebind.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	regCfg := registryTestRegistryConfig()
	regCfg.AppError = configtypes.RPCRegistryAppError{CodeKey: "app-code", TypeKey: "app-type"}
	regCfg.Methods = []configtypes.RPCMethod{
		{
			Method:     "exchange",
			GRPCMethod: registryTestBackendMethod,
			Target:     "backend",
			AuthClass:  configtypes.RPCAuthClassAnonymous,
		},
	}
	cfg.RPC.Registry = regCfg
	h, _, node := newAuthClassTestHandler(t, cfg)
	// Setup wires the RPC handler of the node only when a registry is already set, so the registry is set after it and
	// the call goes to OnRPC directly, as the registry tests do.
	backend := newRegistryTestBackend(t)
	h.SetRPCRegistry(startRegistryTestRegistry(t, regCfg, backend))

	client := connectRevokeTestClient(t, node, "")
	require.Len(t, node.Hub().Connections(), 1)
	require.Empty(t, client.UserID())
	// A connection with no token stores no class; the class it has is anonymous.
	require.Equal(t, rpcregistry.AuthClassAnonymous, effectiveClass(connStateOf(client).identity.Load(), "", ""))

	request := []byte("exchange-request")
	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "exchange", Data: request}, nil)
	require.NoError(t, err)
	// The backend answers with the bytes of the request.
	require.Equal(t, request, reply.Data)
	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, registryTestBackendMethod, calls[0].Method)
	require.Empty(t, calls[0].Metadata.Get("authorization"))

	// The backend refuses the exchange by app type: the anonymous method reports the type of the trailer.
	backend.FailMethod(registryTestBackendMethod, tools.GRPCBytesFailure{
		Err:      status.Error(codes.InvalidArgument, "refused"),
		Trailers: metadata.Pairs("app-type", "REFRESH_TOKEN_REFUSED"),
	})
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "exchange", Data: request}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(400), protoErr.Code)
	require.Equal(t, "REFRESH_TOKEN_REFUSED", protoErr.AppType)

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"meta": map[string]any{"installation": "x"},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", client.UserID())
	require.Equal(t, registryTestClass("authenticated"), connStateOf(client).identity.Load().class)
	require.True(t, client.IsSubscribed(h.cfgContainer.PersonalChannel("42")))

	// The meta claim of the access token replaces the connection's attribution at the rebind.
	require.Equal(t, json.RawMessage(`{"installation":"x"}`), storedMeta(t, client))

	// A later refresh whose token carries another meta claim replaces the attribution of the connection.
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"meta": map[string]any{"installation": "y"},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage(`{"installation":"y"}`), storedMeta(t, client))

	// A refresh whose token carries no meta claim keeps the attribution the connection has, as upstream's refresh does.
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage(`{"installation":"y"}`), storedMeta(t, client))
}

// storedMeta returns the meta the storage of client holds, and fails the test when it holds none.
func storedMeta(t *testing.T, client Client) json.RawMessage {
	t.Helper()
	storage, release := client.AcquireStorage()
	defer release(storage)
	meta, ok := storage[clientstorage.KeyMeta].(json.RawMessage)
	require.True(t, ok)
	return meta
}
