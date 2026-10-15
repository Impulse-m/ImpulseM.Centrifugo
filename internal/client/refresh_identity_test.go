package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindExpiredRule checks the expirations which Core refuses to a rebind. The refresh refuses them before the
// channels of the user go.
func TestRebindExpiredRule(t *testing.T) {
	now := time.Now().Unix()
	require.True(t, rebindExpired(-1))
	require.True(t, rebindExpired(now))
	require.True(t, rebindExpired(now-1))
	require.False(t, rebindExpired(0))
	require.False(t, rebindExpired(now+60))
}

// TestRefreshWithoutRecordChecksAuthClass checks that a connection with no record refuses a token whose auth class is
// not valid for its user, as a connection with a record does and as connect does.
func TestRefreshWithoutRecordChecksAuthClass(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{}, false)
	client, state := newRebindTestClient("", "anonymous")
	state.record = nil

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "bot", revokeTestOtherUUID, nil),
	}, nil)
	require.ErrorIs(t, err, centrifuge.DisconnectInvalidToken)
}

// TestRefreshWithoutRecordReplyHasNoIdentity checks that a refresh of a connection with no record replies as upstream
// does: it carries neither the user nor the auth class.
func TestRefreshWithoutRecordReplyHasNoIdentity(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{}, false)
	client, state := newRebindTestClient("42", "authenticated")
	state.record = nil

	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.User)
	require.Empty(t, reply.AuthClass)
	require.NotNil(t, reply.TokenIdentity)
}

// TestRefreshRefusesAnonymousTokenWhenDisallowed checks that a refresh with an anonymous token is refused with the
// connection kept as it is when disallow_anonymous_connection_tokens is set, as connect refuses such a token. The
// downgrade of a connection with a user is the case which the flag must stop.
func TestRefreshRefusesAnonymousTokenWhenDisallowed(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
	h.cfgContainer.ConfigRef().Client.DisallowAnonymousConnectionTokens = true
	client, state := newRebindTestClient("42", "authenticated")
	state.channels = map[string]centrifuge.ChannelContext{
		"#42": {Source: subsource.UserPersonal},
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.ErrorIs(t, err, centrifuge.DisconnectPermissionDenied)
	require.Empty(t, state.rebinds)
	require.Equal(t, "42", state.user)
	require.Contains(t, state.channels, "#42")
}

// TestRefreshRestoresPersonalChannelAfterRefusedDowngrade checks that a refused rebind to anonymous leaves the connection
// with its user, and that the next refresh with the same user subscribes the personal channel again.
func TestRefreshRestoresPersonalChannelAfterRefusedDowngrade(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
	client, state := newRebindTestClient("42", "authenticated")
	state.channels = map[string]centrifuge.ChannelContext{
		"#42": {Source: subsource.UserPersonal},
	}
	state.rebindErr = centrifuge.ErrRebindExpired

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.ErrorIs(t, err, centrifuge.ErrorTokenExpired)
	require.Equal(t, "42", state.user)
	require.NotContains(t, state.channels, "#42")

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Contains(t, state.channels, "#42")
	require.EqualValues(t, subsource.UserPersonal, state.channels["#42"].Source)
}

// TestTokenUserOfPendingConnection checks that the user of the token of a pending connection is the user of the record,
// which Core does not see, and that a connection without a record has the user Core holds.
func TestTokenUserOfPendingConnection(t *testing.T) {
	client, _ := newRebindTestClientWithToken("", "42", "pending_2fa")
	require.Equal(t, "42", tokenUserOf(client))

	noRecord, state := newRebindTestClient("7", "authenticated")
	state.record = nil
	require.Equal(t, "7", tokenUserOf(noRecord))
}
