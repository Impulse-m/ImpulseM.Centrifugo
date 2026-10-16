package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

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
