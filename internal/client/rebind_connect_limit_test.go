package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindUpgradeTakesUserConnectLimit checks that an upgrade through rebind takes the connect rate limit of the user it
// upgrades to, the bucket a connect of that user takes. A refused take answers the refresh with too many requests and
// keeps the identity of the connection. A refresh which keeps the user takes nothing.
func TestRebindUpgradeTakesUserConnectLimit(t *testing.T) {
	h, _ := newRebindTestHandlerWithUserCommand(t, configtypes.Rebind{Enabled: true}, configtypes.RateLimitUserCommand{
		Enabled: true,
		Connect: rateLimitBucketsOf(time.Minute, 1),
	})
	now := time.Now().Unix()

	// A pending connection of user 45 takes nothing: Core sees no user for it.
	pending, pendingState := newRebindTestClient("", "anonymous")
	_, _, err := h.OnRefresh(pending, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "pending_2fa", revokeTestUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)
	require.Len(t, pendingState.rebinds, 1)
	require.Empty(t, pending.UserID())

	// The upgrade of the pending connection takes the one token of user 45.
	upgraded, upgradedState := newRebindTestClient("", "anonymous")
	_, _, err = h.OnRefresh(upgraded, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "45", upgraded.UserID())
	require.Len(t, upgradedState.rebinds, 1)

	// Another upgrade to user 45 is refused, and the connection keeps its identity.
	refused, refusedState := newRebindTestClient("", "anonymous")
	before := refused.ExtState().(*connState).identity.Load()
	_, _, err = h.OnRefresh(refused, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil)
	require.Equal(t, centrifuge.ErrorTooManyRequests, err)
	require.Empty(t, refusedState.rebinds)
	require.Empty(t, refused.UserID())
	require.Same(t, before, refused.ExtState().(*connState).identity.Load())

	// The connect of user 45 shares the bucket, so it is refused as well.
	_, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c2",
		Token:    rebindTestToken(t, "45", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil, false)
	require.Equal(t, centrifuge.ErrorTooManyRequests, err)

	// A refresh which keeps the user does not take the bucket, so it succeeds with the bucket empty.
	_, _, err = h.OnRefresh(upgraded, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "45", upgraded.UserID())
}

// TestRebindUpgradeRefusedAtChannelLimit checks that an upgrade which would take the connection over client.channel_limit
// is refused before the identity moves: the connection keeps its identity and its channels, and nothing is rebound. An
// upgrade which fits under the limit goes through, the limit itself included.
func TestRebindUpgradeRefusedAtChannelLimit(t *testing.T) {
	now := time.Now().Unix()
	cfg := newRebindTestConfig(configtypes.Rebind{Enabled: true}, configtypes.RateLimitUserCommand{})
	cfg.Client.ChannelLimit = 2
	h, _ := newRebindTestHandlerWithConfig(t, cfg)

	// Two client channels fill the limit, so the personal channel of the upgrade does not fit.
	refused, refusedState := newRebindTestClient("", "anonymous")
	refusedState.channels["chat:one"] = centrifuge.ChannelContext{}
	refusedState.channels["chat:two"] = centrifuge.ChannelContext{}
	before := refused.ExtState().(*connState).identity.Load()
	_, _, err := h.OnRefresh(refused, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestUUID, map[string]any{"iat": now}),
	}, nil)
	require.Equal(t, centrifuge.ErrorLimitExceeded, err)
	require.Empty(t, refusedState.rebinds)
	require.Empty(t, refused.UserID())
	require.Same(t, before, refused.ExtState().(*connState).identity.Load())
	require.Len(t, refusedState.channels, 2)

	// With one channel of headroom the upgrade fits: the personal channel takes the last place.
	upgraded, upgradedState := newRebindTestClient("", "anonymous")
	upgradedState.channels["chat:one"] = centrifuge.ChannelContext{}
	_, _, err = h.OnRefresh(upgraded, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "45", upgraded.UserID())
	require.Len(t, upgradedState.rebinds, 1)
	require.Len(t, upgradedState.channels, 2)
}

// TestRebindUpgradeOfOtherUserTakesItsBucket checks that the take is made for the user the connection upgrades to: a
// user whose bucket is empty does not refuse an upgrade to another user.
func TestRebindUpgradeOfOtherUserTakesItsBucket(t *testing.T) {
	h, _ := newRebindTestHandlerWithUserCommand(t, configtypes.Rebind{Enabled: true}, configtypes.RateLimitUserCommand{
		Enabled: true,
		Connect: rateLimitBucketsOf(time.Minute, 1),
	})
	now := time.Now().Unix()
	first, _ := newRebindTestClient("", "anonymous")
	_, _, err := h.OnRefresh(first, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "46", "authenticated", revokeTestUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)

	second, _ := newRebindTestClient("", "anonymous")
	_, _, err = h.OnRefresh(second, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "47", "authenticated", revokeTestOtherUUID, map[string]any{"iat": now}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "47", second.UserID())
}

// TestRebindToNoUserRefusedAtChannelLimit checks that a rebind to no user, to an anonymous or a pending token, is refused
// before the identity moves when the channels of the token do not fit under client.channel_limit. The connection keeps its
// identity and its channels, and nothing is rebound. A downgrade counts the channels it keeps, so a channel which the
// downgrade drops does not count against the limit.
func TestRebindToNoUserRefusedAtChannelLimit(t *testing.T) {
	subs := map[string]any{"tok-a": map[string]any{}, "tok-b": map[string]any{}}

	cfg := newRebindTestConfig(configtypes.Rebind{Enabled: true}, configtypes.RateLimitUserCommand{})
	cfg.Client.ChannelLimit = 1
	h, _ := newRebindTestHandlerWithConfig(t, cfg)

	// An anonymous connection refreshing to a pending token would take both channels of the token.
	anonymous, anonymousState := newRebindTestClient("", "anonymous")
	before := anonymous.ExtState().(*connState).identity.Load()
	_, _, err := h.OnRefresh(anonymous, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "pending_2fa", revokeTestUUID, map[string]any{"subs": subs}),
	}, nil)
	require.Equal(t, centrifuge.ErrorLimitExceeded, err)
	require.Empty(t, anonymousState.rebinds)
	require.Empty(t, anonymousState.channels)
	require.Same(t, before, anonymous.ExtState().(*connState).identity.Load())

	// A pending connection which holds a channel of its token refreshes to an anonymous token. The downgrade drops that
	// channel, so the two channels of the token do not fit under a limit of one, and they do fit under a limit of two.
	pending, pendingState := newRebindTestClientWithToken("", "42", "pending_2fa")
	pendingState.channels["tok"] = centrifuge.ChannelContext{Source: subsource.ConnectionToken}
	before = pending.ExtState().(*connState).identity.Load()
	_, _, err = h.OnRefresh(pending, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, map[string]any{"subs": subs}),
	}, nil)
	require.Equal(t, centrifuge.ErrorLimitExceeded, err)
	require.Empty(t, pendingState.rebinds)
	require.Len(t, pendingState.channels, 1)
	require.Contains(t, pendingState.channels, "tok")
	require.Same(t, before, pending.ExtState().(*connState).identity.Load())

	cfg.Client.ChannelLimit = 2
	h, _ = newRebindTestHandlerWithConfig(t, cfg)
	fits, fitsState := newRebindTestClientWithToken("", "42", "pending_2fa")
	fitsState.channels["tok"] = centrifuge.ChannelContext{Source: subsource.ConnectionToken}
	_, _, err = h.OnRefresh(fits, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, map[string]any{"subs": subs}),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, fits.UserID())
	require.Len(t, fitsState.rebinds, 1)
	require.NotContains(t, fitsState.channels, "tok")
	require.Contains(t, fitsState.channels, "tok-a")
	require.Contains(t, fitsState.channels, "tok-b")
}
