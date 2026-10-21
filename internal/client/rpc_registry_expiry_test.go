package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// expiryTestToken returns a connection token of user, signed with "secret", with exp as its JWT exp, and expire_at
// when it is not zero.
func expiryTestToken(t *testing.T, user string, exp int64, expireAt int64) string {
	t.Helper()
	claims := map[string]any{"sub": user, "exp": exp}
	if expireAt != 0 {
		claims["expire_at"] = expireAt
	}
	return getConnTokenWithClaims(t, claims)
}

// expiryTestRegistry returns the registry of the expiry tests: the shared test registry, with an anonymous method of a
// neutral name, and the switch as given.
func expiryTestRegistry(refuse bool) configtypes.RPCRegistry {
	reg := registryTestRegistryConfig()
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:     "status.ping",
		GRPCMethod: registryTestBackendMethod,
		Target:     "backend",
		AuthClass:  configtypes.RPCAuthClassAnonymous,
	})
	reg.RefuseExpiredTokens = refuse
	reg.Errors.TokenExpired = expiryTestTokenExpiredError
	return reg
}

// expiryTestTokenExpiredError is errors.token_expired with its default code and message.
var expiryTestTokenExpiredError = configtypes.RPCRegistryError{Code: 109, Message: "token expired"}

// expiryTestHandler starts the registry of reg against backend, and returns the handler which uses it. The config
// carries the client refresh lead which the switch needs, so the switch passes validation.
func expiryTestHandler(t *testing.T, backend *tools.GRPCBytesBackend, reg configtypes.RPCRegistry) (*Handler, *config.Container) {
	t.Helper()
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	cfg.RPC.Registry = reg
	return newRegistryTestHandler(t, cfg, registry)
}

// newExpiryDedupHandler is newDedupHandler with the client refresh lead which the switch needs.
func newExpiryDedupHandler(t *testing.T, reg configtypes.RPCRegistry, backend *dedupBackend) *Handler {
	t.Helper()
	reg.Errors.TokenExpired = expiryTestTokenExpiredError
	registry := startDedupRegistry(t, reg, backend.conn(t))
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.Client.RefreshLead = configtypes.Duration(60 * time.Second)
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	return h
}

// expiryTestClient connects token as user 42 and returns the client with the record of its connection. instance is the
// install instance the client sends, which scopes the calls of a connection without a token session.
func expiryTestClient(t *testing.T, h *Handler, id string, token string, instance string) *rebindClientMock {
	t.Helper()
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: id,
		Token:    token,
	}, nil, false)
	require.NoError(t, err)
	storage := reply.Storage
	if instance != "" {
		storage[rpcInstanceKey] = instance
	}
	client := newRegistryTestClient(id, "42", reply.Context, storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{}
	}
	return client
}

// waitUntilUnix waits until the clock reaches the second exp, polling in 50 ms steps for at most 5 s.
func waitUntilUnix(t *testing.T, exp int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return time.Now().Unix() >= exp
	}, 5*time.Second, 50*time.Millisecond)
}

func TestNextIdentityKeepsTokenExpiry(t *testing.T) {
	identity := nextIdentity(nil, originToken, rpcregistry.AuthClassAuthenticated, "", "tok", jwtverify.ConnectToken{
		UserID:   "42",
		TokenExp: 1700000000,
		ExpireAt: 1800000000,
	})
	require.Equal(t, int64(1700000000), identity.exp)

	identity = nextIdentity(nil, originToken, rpcregistry.AuthClassAuthenticated, "", "tok", jwtverify.ConnectToken{
		UserID:   "42",
		TokenExp: 1800000000,
		ExpireAt: 1700000000,
	})
	require.Equal(t, int64(1800000000), identity.exp)

	identity = nextIdentity(nil, originToken, rpcregistry.AuthClassAuthenticated, "", "", jwtverify.ConnectToken{
		UserID:   "42",
		TokenExp: 1800000000,
	})
	require.Zero(t, identity.exp)

	moved := movedIdentity(identity, centrifuge.TokenIdentity{})
	require.Equal(t, identity.exp, moved.exp)
}

func TestRPCRegistryRefusesExpiredToken(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
	exp := time.Now().Unix() + 2
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, 0), "")
	waitUntilUnix(t, exp)

	callsBefore := testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultExpired))
	errorsBefore := testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "none"))

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(109), protoErr.Code)
	require.Equal(t, "token expired", protoErr.Message)
	require.True(t, protoErr.Temporary)
	require.Equal(t, rpcRetryLater, protoErr.Metadata[rpcRetryMetadataKey])
	require.Empty(t, backend.Calls())
	require.Equal(t, callsBefore+1, testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("users.get", metrics.RPCRegistryResultExpired)))
	require.Equal(t, errorsBefore+1, testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "none")))
}

func TestRPCRegistryForwardsLiveToken(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
	token := expiryTestToken(t, "42", time.Now().Unix()+60, 0)
	client := expiryTestClient(t, h, "c1", token, "")

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, []string{"Bearer " + token}, calls[0].Metadata.Get("authorization"))
}

func TestRPCRegistryExpiredTokenSwitchOff(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(false))
	exp := time.Now().Unix() + 2
	token := expiryTestToken(t, "42", exp, 0)
	client := expiryTestClient(t, h, "c1", token, "")
	waitUntilUnix(t, exp)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, []string{"Bearer " + token}, calls[0].Metadata.Get("authorization"))
}

func TestRPCRegistryExpiredTokenAnonymousMethod(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
	exp := time.Now().Unix() + 2
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, 0), "")
	waitUntilUnix(t, exp)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "status.ping", Data: []byte("x")}, nil)
	require.NoError(t, err)
	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Empty(t, calls[0].Metadata.Get("authorization"))
}

func TestRPCRegistryExpireAtDoesNotDecide(t *testing.T) {
	now := time.Now().Unix()

	t.Run("expire_at later than exp is refused from exp", func(t *testing.T) {
		backend := newRegistryTestBackend(t)
		h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
		exp := now + 2
		client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, now+600), "")
		waitUntilUnix(t, exp)

		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
		require.Equal(t, uint32(109), requireProtocolError(t, err).Code)
		require.Empty(t, backend.Calls())
	})

	t.Run("expire_at earlier than exp does not refuse", func(t *testing.T) {
		backend := newRegistryTestBackend(t)
		h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
		token := expiryTestToken(t, "42", now+600, now+30)
		client := expiryTestClient(t, h, "c1", token, "")

		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
		require.NoError(t, err)
		calls := backend.Calls()
		require.Len(t, calls, 1)
		require.Equal(t, []string{"Bearer " + token}, calls[0].Metadata.Get("authorization"))
	})
}

// TestRPCRegistryAPIRefreshDoesNotRenewToken pins that the expiry of the forwarded token decides, not the expiry of the
// connection: a connection whose expire_at is later than the token's exp is refused once exp has passed. A server API
// refresh moves only the connection's expiry in Core, and never reaches the handler, so the record stays as connected.
func TestRPCRegistryAPIRefreshDoesNotRenewToken(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
	now := time.Now().Unix()
	exp := now + 2
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, now+600), "")
	before := connStateOf(client).identity.Load().exp
	require.Equal(t, exp, before)

	waitUntilUnix(t, exp)
	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(109), requireProtocolError(t, err).Code)
	require.Equal(t, before, connStateOf(client).identity.Load().exp)
	require.Empty(t, backend.Calls())
}

func TestRPCRegistryTokenExpiryFollowsRefresh(t *testing.T) {
	backend := newRegistryTestBackend(t)
	h, _ := expiryTestHandler(t, backend, expiryTestRegistry(true))
	now := time.Now().Unix()
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", now+60, 0), "")

	refreshExp := now + 120
	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{Token: expiryTestToken(t, "42", refreshExp, 0)}, nil)
	require.NoError(t, err)
	require.Equal(t, refreshExp, connStateOf(client).identity.Load().exp)
}

// A refusal of an expired token is not recorded: the retransmit, once the refresh lands, runs the call and records it.
func TestRPCRegistryExpiredTokenRefusalLeavesNoDedupRecord(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.RefuseExpiredTokens = true
	h := newExpiryDedupHandler(t, reg, backend)
	exp := time.Now().Unix() + 2
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, 0), testInstance)
	waitUntilUnix(t, exp)
	msgID := dedupMsgID(7)

	_, err := dedupCall(h, client, "users.get", msgID, "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(109), protoErr.Code)
	require.Equal(t, rpcRetryLater, protoErr.Metadata[rpcRetryMetadataKey])
	require.Empty(t, backend.Calls())

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{Token: expiryTestToken(t, "42", time.Now().Unix()+60, 0)}, nil)
	require.NoError(t, err)
	reply, err := dedupCall(h, client, "users.get", msgID, "x")
	require.NoError(t, err)
	require.False(t, reply.Cached)
	require.Equal(t, []byte("reply:x"), reply.Data)
	require.Len(t, backend.Calls(), 1)
	require.Equal(t, 1.0, testDedupCount(vec, "miss"))
}

// A retransmit of a recorded call is refused while its token has expired, before the record is read. Once the refresh
// lands, it gets the cached answer, and the backend has run once.
func TestRPCRegistryExpiredRetransmitGetsCachedAfterRefresh(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.RefuseExpiredTokens = true
	h := newExpiryDedupHandler(t, reg, backend)
	exp := time.Now().Unix() + 2
	client := expiryTestClient(t, h, "c1", expiryTestToken(t, "42", exp, 0), testInstance)
	msgID := dedupMsgID(8)

	first, err := dedupCall(h, client, "users.get", msgID, "x")
	require.NoError(t, err)
	require.Equal(t, []byte("reply:x"), first.Data)

	waitUntilUnix(t, exp)
	_, err = dedupCall(h, client, "users.get", msgID, "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(109), protoErr.Code)
	require.Equal(t, rpcRetryLater, protoErr.Metadata[rpcRetryMetadataKey])
	require.Len(t, backend.Calls(), 1)

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{Token: expiryTestToken(t, "42", time.Now().Unix()+60, 0)}, nil)
	require.NoError(t, err)
	retransmit, err := dedupCall(h, client, "users.get", msgID, "x")
	require.NoError(t, err)
	require.True(t, retransmit.Cached)
	require.Equal(t, []byte("reply:x"), retransmit.Data)
	require.Len(t, backend.Calls(), 1)
}
