package client

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const registryTestBackendMethod = "/example.v1.UserService/GetUser"

// newRegistryTestBackend starts a shared bytes backend which is closed when the test ends.
func newRegistryTestBackend(t testing.TB) *tools.GRPCBytesBackend {
	t.Helper()
	backend := tools.NewGRPCBytesBackend()
	t.Cleanup(backend.Close)
	return backend
}

// newRegistryTestClient returns a client for the registry tests. Its storage starts with a copy of the storage of a
// connect reply, and the record in it is adopted as the ext state of the client, as Core does after a connect.
func newRegistryTestClient(id string, userID string, ctx context.Context, storage map[string]any) *rebindClientMock {
	client := &rebindClientMock{}
	client.IDFunc = func() string { return id }
	client.UserIDFunc = func() string { return userID }
	client.ContextFunc = func() context.Context {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}
	client.TransportFunc = func() centrifuge.TransportInfo { return nil }
	if len(storage) > 0 {
		copied, release := client.AcquireStorage()
		for key, value := range storage {
			if key != storageKeyConnState {
				copied[key] = value
			}
		}
		release(copied)
	}
	if rec, ok := storage[storageKeyConnState].(*connState); ok {
		client.ExtStateFunc = func() any { return rec }
	}
	return client
}

// connectReplyIdentity returns the identity which the record of a connect reply holds.
func connectReplyIdentity(t *testing.T, reply centrifuge.ConnectReply) *connIdentity {
	t.Helper()
	rec, ok := reply.Storage[storageKeyConnState].(*connState)
	require.True(t, ok, "the connect reply carries no record")
	return rec.identity.Load()
}

// registryTestRegistryConfig returns the registry config which the handler and the registry share.
func registryTestRegistryConfig() configtypes.RPCRegistry {
	return configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass:        configtypes.RPCAuthClassAuthenticated,
			Timeout:          configtypes.Duration(time.Second),
			MaxRequestBytes:  16,
			MaxResponseBytes: 1 << 16,
		},
		Targets: []configtypes.RPCTarget{
			{
				Name:            "backend",
				Endpoint:        "dns:///backend.example:9000",
				EmulatedHeaders: []string{"x-example-init"},
			},
		},
		Methods: []configtypes.RPCMethod{
			{
				Method:     "users.get",
				GRPCMethod: registryTestBackendMethod,
				Target:     "backend",
			},
			{
				Method:           "users.small",
				GRPCMethod:       registryTestBackendMethod,
				Target:           "backend",
				MaxResponseBytes: 100,
			},
			{
				Method:     "help.config",
				GRPCMethod: "/example.v1.HelpService/GetConfig",
				Target:     "backend",
				AuthClass:  configtypes.RPCAuthClassAnonymous,
			},
		},
		Errors: configtypes.RPCRegistryErrors{
			UnknownMethod:   configtypes.RPCRegistryError{Code: 104, Message: "method not found"},
			AuthClass:       configtypes.RPCRegistryError{Code: 103, Message: "permission denied"},
			RequestTooLarge: configtypes.RPCRegistryError{Code: 106, Message: "limit exceeded"},
			Cancelled:       configtypes.RPCRegistryError{Code: 107, Message: "bad request"},
		},
	}
}

// newRegistryTestHandler returns a handler with cfg, which uses registry for RPC calls.
func newRegistryTestHandler(t testing.TB, cfg config.Config, registry *rpcregistry.Registry) (*Handler, *config.Container) {
	t.Helper()
	return newRegistryTestHandlerWithClaim(t, cfg, registry, "")
}

// newRegistryTestHandlerWithClaim is newRegistryTestHandler whose connection token verifier reads the
// auth class from authClassClaim.
func newRegistryTestHandlerWithClaim(t testing.TB, cfg config.Config, registry *rpcregistry.Registry, authClassClaim string) (*Handler, *config.Container) {
	t.Helper()
	node := tools.NodeWithMemoryEngine()
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{
		HMACSecretKey:  "secret",
		AuthClassClaim: authClassClaim,
	}, cfgContainer)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetRPCRegistry(registry)
	return h, cfgContainer
}

// startRegistryTestRegistry returns a registry with cfg applied, whose targets use the backend.
func startRegistryTestRegistry(t testing.TB, cfg configtypes.RPCRegistry, backend *tools.GRPCBytesBackend) *rpcregistry.Registry {
	t.Helper()
	conn, err := backend.Conn()
	require.NoError(t, err)
	dial := func(*rpcregistry.Target) (io.Closer, error) {
		return conn, nil
	}
	registry, err := rpcregistry.Start(cfg, dial)
	require.NoError(t, err)
	return registry
}

// stubRPCProxy returns a namespace RPC proxy which answers with data and counts its calls.
func stubRPCProxy(calls *int, data string) proxy.RPCHandlerFunc {
	return func(proxy.Client, centrifuge.RPCEvent, *config.Container, proxy.PerCallData) (centrifuge.RPCReply, error) {
		*calls++
		return centrifuge.RPCReply{Data: []byte(data)}, nil
	}
}

func TestRPCRegistryDisabledLeavesCallsAsTheyWere(t *testing.T) {
	cfg := config.DefaultConfig()
	client := newRegistryTestClient("c1", "42", nil, nil)
	event := centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}

	// Without a registry, RPC without a handler answers the protocol default.
	h, _ := newRegistryTestHandler(t, cfg, nil)
	_, err := h.OnRPC(client, event, nil)
	require.ErrorIs(t, err, centrifuge.ErrorNotAvailable)

	// A disabled registry never answers, and the namespace proxy keeps handling the call.
	disabled, err := rpcregistry.Start(configtypes.RPCRegistry{}, nil)
	require.NoError(t, err)
	h, _ = newRegistryTestHandler(t, cfg, disabled)
	calls := 0
	reply, err := h.OnRPC(client, event, stubRPCProxy(&calls, "proxied"))
	require.NoError(t, err)
	require.Equal(t, []byte("proxied"), reply.Data)
	require.Equal(t, 1, calls)
}

func TestRPCRegistryUnknownMethodAndNamespaceFallback(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	cfg.RPC.Namespaces = configtypes.RPCNamespaces{
		{Name: "ns", RpcOptions: configtypes.RpcOptions{ProxyEnabled: true}},
	}
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "nope.method"}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(104), protoErr.Code)
	require.Equal(t, "method not found", protoErr.Message)

	// A method which a namespace proxy handles keeps going to the proxy.
	calls := 0
	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "ns:thing"}, stubRPCProxy(&calls, "proxied"))
	require.NoError(t, err)
	require.Equal(t, []byte("proxied"), reply.Data)
	require.Equal(t, 1, calls)
}

func TestRPCRegistryCallChecks(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	h, _ := newRegistryTestHandler(t, cfg, registry)

	anonymous := newRegistryTestClient("c1", "", nil, nil)
	authenticated := newRegistryTestClient("c2", "42", nil, nil)

	// An anonymous connection cannot call an authenticated method.
	_, err := h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(103), protoErr.Code)

	// The request size gate is checked before the call.
	_, err = h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "users.get", Data: make([]byte, 17)}, nil)
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(106), protoErr.Code)
	require.Empty(t, backend.Calls())

	// An anonymous method is callable by everyone, and the raw bytes come back.
	reply, err := h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "help.config", Data: []byte("raw")}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("raw"), reply.Data)
	require.Len(t, backend.Calls(), 1)
}

// A real call whose reply is over the response limit of the method maps to the request-too-large
// error. The check pins the gRPC receive-overflow error: grpc has no typed error for it, so a
// grpc upgrade which changes the text fails this test.
func TestRPCRegistryResponseOverLimitIsRequestTooLarge(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.small", Data: []byte(tools.GRPCBytesBigPayload)}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(106), protoErr.Code)
	require.Equal(t, "limit exceeded", protoErr.Message)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryAuthorizationFollowsConnectionToken(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	h, cfgContainer := newRegistryTestHandler(t, cfg, registry)
	require.NotNil(t, cfgContainer)

	connectToken := getConnTokenHS("42", 0)
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    connectToken,
		Headers:  map[string]string{"x-example-init": "init"},
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, connectToken, connectReplyIdentity(t, reply).token)
	require.Equal(t, "init", connectionEmulatedHeader(t, reply.Context, "x-example-init"))

	// The connection context carries the connect-time headers, as it does for a real client.
	client := newRegistryTestClient("c1", "42", reply.Context, reply.Storage)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "help.config", Data: []byte("x")}, nil)
	require.NoError(t, err)

	calls := backend.Calls()
	require.Len(t, calls, 2)
	require.Equal(t, []string{"Bearer " + connectToken}, calls[0].Metadata.Get("authorization"))
	require.Equal(t, []string{"init"}, calls[0].Metadata.Get("x-example-init"))
	require.Empty(t, calls[1].Metadata.Get("authorization"))

	// A refresh replaces the stored token, so later calls carry the new one.
	refreshToken := getConnTokenHS("42", time.Now().Add(time.Hour).Unix())
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{Token: refreshToken}, nil)
	require.NoError(t, err)
	require.Equal(t, refreshToken, connStateOf(client).identity.Load().token)
}

// connectionEmulatedHeader returns a connect-time header kept in the connection context.
func connectionEmulatedHeader(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	require.NotNil(t, ctx)
	headers, ok := clientcontext.GetEmulatedHeadersFromContext(ctx)
	require.True(t, ok)
	return headers[name]
}

func TestRPCRegistryTokenNotStoredWhenDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	h, _ := newRegistryTestHandler(t, cfg, nil)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenHS("42", 0),
	}, nil, false)
	require.NoError(t, err)
	// Nothing needs a record, so the token is not kept anywhere.
	require.NotContains(t, reply.Storage, storageKeyConnState)
}

// newRegistryFlagNode returns a real Core node which uses registry, for the connect flag tests.
func newRegistryFlagNode(t *testing.T, registry *rpcregistry.Registry) *centrifuge.Node {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetRPCRegistry(registry)
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return node
}

func TestRPCRegistryConnectAdvertisesRPCOptionsWhenClientAsksForIt(t *testing.T) {
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), newRegistryTestBackend(t))

	_, _, result := connectTestClient(t, newRegistryFlagNode(t, registry), centrifuge.ConnectionFlagRPCOptions)
	require.NotZero(t, result.Flag&centrifuge.ConnectionFlagRPCOptions)
}

func TestRPCRegistryConnectNoRPCOptionsWhenClientDoesNotAskForIt(t *testing.T) {
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), newRegistryTestBackend(t))

	_, _, result := connectTestClient(t, newRegistryFlagNode(t, registry), 0)
	require.Zero(t, result.Flag&centrifuge.ConnectionFlagRPCOptions)
}

func TestRPCRegistryConnectNoRPCOptionsWhenRegistryDisabled(t *testing.T) {
	_, _, result := connectTestClient(t, newRegistryFlagNode(t, nil), centrifuge.ConnectionFlagRPCOptions)
	require.Zero(t, result.Flag&centrifuge.ConnectionFlagRPCOptions)
}

func TestRPCRegistryConnectNoRPCOptionsWhenRegistryConfiguredButDisabled(t *testing.T) {
	reg := registryTestRegistryConfig()
	reg.Enabled = false
	registry := startRegistryTestRegistry(t, reg, newRegistryTestBackend(t))
	require.False(t, registry.Enabled())

	_, _, result := connectTestClient(t, newRegistryFlagNode(t, registry), centrifuge.ConnectionFlagRPCOptions)
	require.Zero(t, result.Flag&centrifuge.ConnectionFlagRPCOptions)
}

// registryAuthClassTestRegistry returns the registry config of the auth class tests: the test registry
// with methods of class pending_2fa and bot, and the auth class claim set.
func registryAuthClassTestRegistry() configtypes.RPCRegistry {
	reg := registryTestRegistryConfig()
	reg.AuthClassClaim = "auth_class"
	reg.Methods = append(reg.Methods,
		configtypes.RPCMethod{
			Method:     "users.checkPassword",
			GRPCMethod: registryTestBackendMethod,
			Target:     "backend",
			AuthClass:  configtypes.RPCAuthClassPending2FA,
		},
		configtypes.RPCMethod{
			Method:     "bots.answer",
			GRPCMethod: registryTestBackendMethod,
			Target:     "backend",
			AuthClass:  configtypes.RPCAuthClassBot,
		},
	)
	return reg
}

// newRegistryAuthClassHandler returns a handler which reads the auth class claim, and the backend of its registry.
func newRegistryAuthClassHandler(t *testing.T) (*Handler, *tools.GRPCBytesBackend) {
	t.Helper()
	backend := newRegistryTestBackend(t)
	reg := registryAuthClassTestRegistry()
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, registry, "auth_class")
	return h, backend
}

func TestRPCRegistryPending2FAConnectionCallsOnlyItsClass(t *testing.T) {
	h, backend := newRegistryAuthClassHandler(t)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "pending_2fa"}),
	}, nil, false)
	require.NoError(t, err)
	id := connectReplyIdentity(t, reply)
	require.Equal(t, rpcregistry.AuthClassPending2FA, id.class)
	require.Equal(t, "auth_class", id.claim)

	client := newRegistryTestClient("c1", "42", reply.Context, reply.Storage)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "bots.answer", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	require.Empty(t, backend.Calls())

	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.checkPassword", Data: []byte("x")}, nil)
	require.NoError(t, err)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryBotMethodNeedsBotClass(t *testing.T) {
	h, backend := newRegistryAuthClassHandler(t)

	// A connection of an authenticated user, with no class stored, is not a bot.
	authenticated := newRegistryTestClient("c1", "42", nil, nil)
	_, err := h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "bots.answer", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	require.Empty(t, backend.Calls())

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "bot"}),
	}, nil, false)
	require.NoError(t, err)
	bot := newRegistryTestClient("c2", "42", reply.Context, reply.Storage)
	_, err = h.OnRPC(bot, centrifuge.RPCEvent{Method: "bots.answer", Data: []byte("x")}, nil)
	require.NoError(t, err)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryConnectionWithoutStoredClassFallsBackToUser(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	h, _ := newRegistryTestHandler(t, cfg, registry)

	// Without a claim, a client with no stored class, such as one connected through a connect proxy,
	// has the class of its user.
	user := newRegistryTestClient("c1", "42", nil, nil)
	_, err := h.OnRPC(user, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)

	anonymous := newRegistryTestClient("c2", "", nil, nil)
	_, err = h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	reply, err := h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "help.config", Data: []byte("raw")}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("raw"), reply.Data)
	require.Len(t, backend.Calls(), 2)
}

func TestRPCRegistryConnectionWithoutStoredClassIsAnonymousWithClaim(t *testing.T) {
	h, backend := newRegistryAuthClassHandler(t)

	// With a claim configured, a client with no stored class cannot prove a class: it is anonymous.
	user := newRegistryTestClient("c1", "42", nil, nil)
	_, err := h.OnRPC(user, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	reply, err := h.OnRPC(user, centrifuge.RPCEvent{Method: "help.config", Data: []byte("raw")}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("raw"), reply.Data)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryConfiguredClaimAbsentGivesUserClass(t *testing.T) {
	h, backend := newRegistryAuthClassHandler(t)

	// A token without the configured claim is not a half-logged-in one: with a user it is authenticated,
	// without a user it is anonymous.
	withUser, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, rpcregistry.AuthClassAuthenticated, connectReplyIdentity(t, withUser).class)
	client := newRegistryTestClient("c1", "42", withUser.Context, withUser.Storage)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "bots.answer", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)

	withoutUser, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, rpcregistry.AuthClassAnonymous, connectReplyIdentity(t, withoutUser).class)
	anonymous := newRegistryTestClient("c2", "", withoutUser.Context, withoutUser.Storage)
	_, err = h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryClaimSetByReloadMakesOldConnectionsAnonymous(t *testing.T) {
	backend := newRegistryTestBackend(t)
	registry := startRegistryTestRegistry(t, registryTestRegistryConfig(), backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	h, cfgContainer := newRegistryTestHandler(t, cfg, registry)

	// Connected before the claim is configured: the class is derived from the user.
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}, nil, false)
	require.NoError(t, err)
	client := newRegistryTestClient("c1", "42", reply.Context, reply.Storage)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)

	// The reload sets the claim on the config, the verifier and the registry, as a reload does.
	reloaded := registryAuthClassTestRegistry()
	cfg.RPC.Registry = reloaded
	require.NoError(t, cfgContainer.Reload(cfg))
	require.NoError(t, h.tokenVerifier.Reload(jwtverify.VerifierConfig{HMACSecretKey: "secret", AuthClassClaim: "auth_class"}))
	apply, err := registry.Prepare(reloaded)
	require.NoError(t, err)
	apply()

	// Until a refresh, the connection is anonymous: it cannot call a method of its user's class.
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "help.config", Data: []byte("raw")}, nil)
	require.NoError(t, err)
	require.Len(t, backend.Calls(), 2)

	// A refresh derives the class under the new claim.
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "authenticated"}),
	}, nil)
	require.NoError(t, err)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
}

func TestRPCRegistryRefreshFromPendingUpgradesGate(t *testing.T) {
	h := newRegistryRebindHandler(t)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "pending_2fa"}),
	}, nil, false)
	require.NoError(t, err)
	// Core sees a pending connection as anonymous, so its upgrade is a rebind.
	client := newRegistryTestClient("c1", "", reply.Context, reply.Storage)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.Equal(t, uint32(103), requireProtocolError(t, err).Code)

	// A refresh with an invalid class is rejected, and the stored class stays the one of the connection.
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "anonymous"}),
	}, nil)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)
	id := connStateOf(client).identity.Load()
	require.Equal(t, rpcregistry.AuthClassPending2FA, id.class)
	require.Equal(t, "auth_class", id.claim)

	// A refresh with a full token upgrades the connection.
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "authenticated"}),
	}, nil)
	require.NoError(t, err)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
}

func TestRPCRegistryConnectRejectsInvalidAuthClass(t *testing.T) {
	h, _ := newRegistryAuthClassHandler(t)
	testCases := []struct {
		name   string
		claims map[string]any
	}{
		{
			name:   "anonymous class with a user",
			claims: map[string]any{"sub": "42", "auth_class": "anonymous"},
		},
		{
			name:   "bot class without a user",
			claims: map[string]any{"auth_class": "bot"},
		},
		{
			name:   "authenticated class without a user",
			claims: map[string]any{"auth_class": "authenticated"},
		},
		{
			name:   "unknown class",
			claims: map[string]any{"sub": "42", "auth_class": "root"},
		},
		{
			name:   "class which is not a string",
			claims: map[string]any{"sub": "42", "auth_class": 1},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
				Token: getConnTokenWithClaims(t, tc.claims),
			}, nil, false)
			require.Equal(t, centrifuge.DisconnectInvalidToken, err)
		})
	}
}
