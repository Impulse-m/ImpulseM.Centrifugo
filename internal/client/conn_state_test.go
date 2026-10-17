package client

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestConnStateSize checks the size budgets of the connection record and its identity, see connState.
func TestConnStateSize(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(connState{}), uintptr(72))
	require.LessOrEqual(t, unsafe.Sizeof(connIdentity{}), uintptr(120))
}

// TestNextIdentityAccountRule checks the account and the origin a new identity of a connection keeps.
func TestNextIdentityAccountRule(t *testing.T) {
	session := "7c1f0e2d-3b4a-4c5d-8e6f-9a0b1c2d3e4f"
	connect := jwtverify.ConnectToken{UserID: "42", Session: session}
	logout := jwtverify.ConnectToken{Session: session}
	other := jwtverify.ConnectToken{UserID: "43", Session: session}

	first := nextIdentity(nil, originToken, rpcregistry.AuthClassAuthenticated, "", "", connect)
	require.Equal(t, "42", first.account)
	require.Equal(t, "42", first.tokenUser)

	afterLogout := nextIdentity(first, originToken, rpcregistry.AuthClassAnonymous, "", "", logout)
	require.Equal(t, "42", afterLogout.account, "a logout keeps the account")
	require.Empty(t, afterLogout.tokenUser)

	afterOther := nextIdentity(afterLogout, originToken, rpcregistry.AuthClassAuthenticated, "", "", other)
	require.Equal(t, "43", afterOther.account, "a token with a user is the account it gives; the check refuses the change first")

	proxy := nextIdentity(nil, originProxy, 0, "", "", jwtverify.ConnectToken{UserID: "42"})
	renewed := nextIdentity(proxy, originToken, rpcregistry.AuthClassAuthenticated, "", "", connect)
	require.Equal(t, originProxy, renewed.origin, "the origin never changes")
	require.Equal(t, rpcregistry.AuthClass(0), proxy.class, "a proxy connection has no class of its own")
}

// connectRecordOf runs the connect of token with h and returns the record the reply carries, nil when it carries none.
func connectRecordOf(t *testing.T, h *Handler, token string) *connState {
	t.Helper()
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    token,
	}, nil, false)
	require.NoError(t, err)
	rec, _ := reply.Storage[storageKeyConnState].(*connState)
	return rec
}

// TestDefaultConnectPathHoldsNothing checks that a connect with every feature off puts nothing into the reply storage
// and allocates no record, and that its reply carries no user, as upstream's reply does. The connected client has no
// ext state either, so the default path pays nothing for the connection state.
func TestDefaultConnectPathHoldsNothing(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenHS("42", 0),
	}, nil, false)
	require.NoError(t, err)
	require.Empty(t, reply.Storage)
	require.Empty(t, reply.User)

	require.NoError(t, h.Setup())
	client := connectRevokeTestClient(t, node, getConnTokenHS("42", 0))
	require.Equal(t, "42", client.UserID())
	require.Nil(t, client.ExtState())
}

// TestConnStateOnlyWithFeatures checks that each condition of needsConnState creates a record on its own, and that
// no feature creates none. The access store alone creates one only for a connection with no user and a token session.
func TestConnStateOnlyWithFeatures(t *testing.T) {
	tests := []struct {
		name      string
		feature   string
		anonymous bool
		noSession bool
		wantRec   bool
	}{
		{name: "no feature", feature: "", wantRec: false},
		{name: "access revocation, user token", feature: "revoke", wantRec: false},
		{name: "access revocation, anonymous token with session", feature: "revoke", anonymous: true, wantRec: true},
		{name: "access revocation, anonymous token without session", feature: "revoke", anonymous: true, noSession: true, wantRec: false},
		{name: "client command limit", feature: "client_command", wantRec: true},
		{name: "user command limit", feature: "user_command", wantRec: true},
		{name: "registry", feature: "registry", wantRec: true},
		{name: "rebind", feature: "rebind", wantRec: true},
		{name: "auth class claim", feature: "claim", wantRec: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newFeatureTestHandler(t, tt.feature)
			claims := map[string]any{"sub": "42", "jti": revokeTestUUID}
			if tt.anonymous {
				delete(claims, "sub")
			}
			if tt.noSession {
				delete(claims, "jti")
			}
			rec := connectRecordOf(t, h, getConnTokenWithClaims(t, claims))
			require.Equal(t, tt.wantRec, rec != nil)
		})
	}
}

// TestDefaultConnectWithUserHoldsNothing checks that a connection with a user has no record and no storage key with the
// production access store on, as the defaults turn it on. Such a connection is checked through its Core user and token
// identity, and its connect and its OnConnect take no storage.
func TestDefaultConnectWithUserHoldsNothing(t *testing.T) {
	cfg := config.DefaultConfig()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	claims := map[string]any{"sub": "42", "jti": revokeTestUUID}
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenWithClaims(t, claims),
	}, nil, false)
	require.NoError(t, err)
	require.NotContains(t, reply.Storage, storageKeyConnState)
	require.False(t, h.needsConnState(cfgContainer.ConfigRef(), reply.Credentials.UserID, reply.Credentials.TokenIdentity))
}

// TestDefaultConnectAnonymousHoldsNothing checks that an anonymous connection without a token session holds no record
// with the production access store on, as the defaults turn it on. Its connect puts no storage key and its OnConnect
// sets no ext state, so it pays nothing for the access revocation it has no session for.
func TestDefaultConnectAnonymousHoldsNothing(t *testing.T) {
	cfg := config.DefaultConfig()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	claims := map[string]any{}
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenWithClaims(t, claims),
	}, nil, false)
	require.NoError(t, err)
	require.NotContains(t, reply.Storage, storageKeyConnState)

	require.NoError(t, h.Setup())
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, claims))
	require.Empty(t, client.UserID())
	require.Nil(t, client.ExtState())
}

// newFeatureTestHandler returns a handler with the one feature of TestConnStateOnlyWithFeatures switched on.
func newFeatureTestHandler(t *testing.T, feature string) *Handler {
	t.Helper()
	cfg := config.DefaultConfig()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	var registry *rpcregistry.Registry
	switch feature {
	case "client_command":
		cfg.Client.RateLimit.ClientCommand.Enabled = true
	case "user_command":
		cfg.Client.RateLimit.UserCommand.Enabled = true
	case "registry":
		cfg.RPC.Registry = registryTestRegistryConfig()
		registry = startRegistryTestRegistry(t, cfg.RPC.Registry, newRegistryTestBackend(t))
	case "rebind":
		cfg.Client.Rebind.Enabled = true
	case "claim":
		cfg.RPC.Registry.AuthClassClaim = "auth_class"
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{
		HMACSecretKey:  "secret",
		AuthClassClaim: "auth_class",
	}, cfgContainer)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetRPCRegistry(registry)
	if feature == "revoke" {
		h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	}
	require.NoError(t, h.setupRateLimit(cfgContainer.ConfigRef()))
	return h
}

// registryCallOnlyClient is a connection whose storage may not be read: a registry call must read its token and class
// from the record alone.
type registryCallOnlyClient struct {
	rebindClientMock
}

// AcquireStorage fails the test, as a registry call must not touch the storage.
func (c *registryCallOnlyClient) AcquireStorage() (map[string]any, func(map[string]any)) {
	panic("a registry call read the connection storage")
}

// TestRegistryCallReadsRecordWithoutStorage checks that a registry call takes the class of a pending connection from its
// record, and that the storage of the connection is not touched.
func TestRegistryCallReadsRecordWithoutStorage(t *testing.T) {
	h, backend := newRegistryAuthClassHandler(t)
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "pending_2fa"}),
	}, nil, false)
	require.NoError(t, err)

	client := &registryCallOnlyClient{}
	client.IDFunc = func() string { return "c1" }
	client.UserIDFunc = func() string { return "" }
	client.ContextFunc = func() context.Context { return context.Background() }
	client.ExtStateFunc = func() any { return reply.Storage[storageKeyConnState] }

	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.checkPassword", Data: []byte("x")}, nil)
	require.NoError(t, err)
	require.Len(t, backend.Calls(), 1)
}

// TestPendingConnectHasNoInfo checks that a pending connection gets no connection info from its token: Core shows the
// info to other clients, and a pending connection is anonymous to them.
func TestPendingConnectHasNoInfo(t *testing.T) {
	h, _, _ := newPendingTestHandler(t)
	claims := map[string]any{"sub": "42", "jti": revokeTestUUID, "info": map[string]any{"name": "x"}}

	claims["auth_class"] = "pending_2fa"
	pending, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenWithClaims(t, claims),
	}, nil, false)
	require.NoError(t, err)
	require.Nil(t, pending.Credentials.Info)

	claims["auth_class"] = "authenticated"
	authenticated, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenWithClaims(t, claims),
	}, nil, false)
	require.NoError(t, err)
	require.NotNil(t, authenticated.Credentials.Info)
}

// BenchmarkOnClientConnectingDefault measures a token connect with every feature off, the path every connection takes
// without the features. It is run on master and on the branch, and the numbers are the same.
func BenchmarkOnClientConnectingDefault(b *testing.B) {
	benchmarkOnClientConnectingDefault(b, false)
}

// BenchmarkOnClientConnectingDefaultStore is the connect of BenchmarkOnClientConnectingDefault with the access store set,
// as the defaults set it. A connection with a user gets no record, so the numbers must match the ones without the store.
func BenchmarkOnClientConnectingDefaultStore(b *testing.B) {
	benchmarkOnClientConnectingDefault(b, true)
}

// benchmarkOnClientConnectingDefault runs the token connect of a user with every feature off, and with the access store
// when withStore is set.
func benchmarkOnClientConnectingDefault(b *testing.B, withStore bool) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	b.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(b, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(b, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	if withStore {
		h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	}
	event := centrifuge.ConnectEvent{ClientID: "c1", Token: getConnTokenHS("42", 0)}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.OnClientConnecting(ctx, event, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}
