package client

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
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

// TestRefreshKeepsConnectionWhenPersonalNamespaceIsRemoved checks that a refresh with the same user does not disconnect
// the connection when a config reload removed the namespace of its personal channel, which the connection is not
// subscribed to. The connection keeps its channels.
func TestRefreshKeepsConnectionWhenPersonalNamespaceIsRemoved(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	h.cfgContainer.ConfigRef().Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace = "removed"
	client, state := newRebindTestClient("42", "authenticated")
	state.channels = map[string]centrifuge.ChannelContext{
		"other": {Source: subsource.ClientAllowed},
	}

	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, reply.TokenIdentity)
	require.Equal(t, "42", state.user)
	require.Contains(t, state.channels, "other")
	require.NotContains(t, state.channels, "removed:#42")
}

// TestProxyRefreshMovesTheRecordIdentity checks that a refresh proxy which answers a new token identity moves the identity
// of the connection record. A revocation of the new session then fails the connection check, and the record holds the
// identity of the new session, not the one of the connect.
func TestProxyRefreshMovesTheRecordIdentity(t *testing.T) {
	h, store := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, _ := newRebindTestClient("42", "authenticated")
	now := time.Now().Unix()
	moved := accessrevoke.Identity(revokeTestOtherUUID, 0, now-10)
	refreshProxy := func(proxy.Client, centrifuge.RefreshEvent, proxy.PerCallData) (centrifuge.RefreshReply, proxy.RefreshExtra, error) {
		return centrifuge.RefreshReply{TokenIdentity: &moved}, proxy.RefreshExtra{}, nil
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{}, refreshProxy)
	require.NoError(t, err)
	userID, identity := connStateOf(client).AccessIdentity()
	require.Equal(t, "42", userID)
	require.Equal(t, moved, identity)

	require.NoError(t, store.RevokeSession(revokeTestOtherUUID, now, 0, nil))
	result := store.Check(userID, identity, accessrevoke.CheckOptions{Session: true})
	require.Equal(t, accessrevoke.Revoked, result.Verdict)
}

// TestProxyRefreshesConcurrentBothSucceed runs two refreshes with the refresh proxy of one connection at the same time.
// The proxy answers each of them with a new token identity, and both refreshes are answered. The identity of the
// connection is one of the two identities, the one the last move stored.
func TestProxyRefreshesConcurrentBothSucceed(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, _ := newRebindTestClient("42", "authenticated")
	now := time.Now().Unix()
	identities := []centrifuge.TokenIdentity{
		accessrevoke.Identity(revokeTestUUID, 0, now-10),
		accessrevoke.Identity(revokeTestOtherUUID, 0, now-10),
	}
	var calls atomic.Int32
	allInProxy := make(chan struct{})
	refreshProxy := func(proxy.Client, centrifuge.RefreshEvent, proxy.PerCallData) (centrifuge.RefreshReply, proxy.RefreshExtra, error) {
		n := calls.Add(1)
		if n == int32(len(identities)) {
			close(allInProxy)
		}
		// Both refreshes are inside the proxy before either returns, so they run at the same time. The wait is bounded, so a
		// refresh which is not admitted to the proxy fails the test instead of blocking it.
		select {
		case <-allInProxy:
		case <-time.After(5 * time.Second):
		}
		moved := identities[n-1]
		return centrifuge.RefreshReply{TokenIdentity: &moved}, proxy.RefreshExtra{}, nil
	}

	var group sync.WaitGroup
	errs := make([]error, len(identities))
	for i := range identities {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			_, _, errs[i] = h.OnRefresh(client, centrifuge.RefreshEvent{}, refreshProxy)
		}(i)
	}
	group.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, int32(len(identities)), calls.Load())
	require.Contains(t, identities, connStateOf(client).identity.Load().identity)
}

// TestProxyMovesOfOneConnectionEndOnTheRecordSession holds the first of two moves of one connection between the store of
// its identity and the sync of the session index, and starts the second move while the first is held. The second move
// waits for the first, so the index ends on the session the record holds, which is the one of the second move.
func TestProxyMovesOfOneConnectionEndOnTheRecordSession(t *testing.T) {
	h, store := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, _ := newRebindTestClient("", "")
	now := time.Now().Unix()
	first := accessrevoke.Identity(revokeTestUUID, 0, now-10)
	second := accessrevoke.Identity(revokeTestOtherUUID, 0, now-10)
	identities := []centrifuge.TokenIdentity{first, second}
	var calls atomic.Int32
	refreshProxy := func(proxy.Client, centrifuge.RefreshEvent, proxy.PerCallData) (centrifuge.RefreshReply, proxy.RefreshExtra, error) {
		moved := identities[calls.Add(1)-1]
		return centrifuge.RefreshReply{TokenIdentity: &moved}, proxy.RefreshExtra{}, nil
	}

	held := make(chan struct{})
	release := make(chan struct{})
	var holds atomic.Int32
	hook := func(rebindableClient) {
		if holds.Add(1) == 1 {
			close(held)
			<-release
		}
	}
	testHookProxyMoved.Store(&hook)
	t.Cleanup(func() { testHookProxyMoved.Store(nil) })

	var group sync.WaitGroup
	errs := make([]error, len(identities))
	group.Add(1)
	go func() {
		defer group.Done()
		_, _, errs[0] = h.OnRefresh(client, centrifuge.RefreshEvent{}, refreshProxy)
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the first move did not reach the hook")
	}

	group.Add(1)
	go func() {
		defer group.Done()
		_, _, errs[1] = h.OnRefresh(client, centrifuge.RefreshEvent{}, refreshProxy)
	}()
	// The second move waits for the lock of the first one. The sleep only gives it the chance to run ahead of the first
	// move, which the lock must stop.
	time.Sleep(50 * time.Millisecond)
	close(release)
	group.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, second, connStateOf(client).identity.Load().identity)
	require.Empty(t, store.SessionConnections().Connections(first.Session))
	require.Equal(t, []accessrevoke.Connection{client}, store.SessionConnections().Connections(second.Session))
}

// TestSameUserClassChangeOfProxyConnectionIsDenied refreshes a connection made through a connect proxy with a token of its
// own user and another class. The class of such a connection is its effective class, so the token is refused and the
// connection keeps its record as it was.
func TestSameUserClassChangeOfProxyConnectionIsDenied(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClient("42", "authenticated")
	next := *state.record.identity.Load()
	next.origin = originProxy
	next.class = 0
	next.claim = ""
	next.token = ""
	state.record.identity.Store(&next)

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "bot", revokeTestOtherUUID, nil),
	}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
	id := state.record.identity.Load()
	require.Equal(t, originProxy, id.origin)
	require.Zero(t, id.class)
	require.Equal(t, "42", state.user)
}

// TestRefreshRestoresIdentityWhenPersonalChannelFails checks that a same-user refresh whose personal channel cannot be
// subscribed keeps the identity the connection had: the new identity is stored back before the disconnect.
func TestRefreshRestoresIdentityWhenPersonalChannelFails(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
	client, state := newRebindTestClient("42", "authenticated")
	prev := state.record.identity.Load().identity
	client.SubscribeFunc = func(string, ...centrifuge.SubscribeOption) error {
		return errors.New("subscribe failed")
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.Equal(t, centrifuge.DisconnectServerError, err)
	require.Equal(t, prev, state.record.identity.Load().identity)
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
