package client

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const registryTestBackendMethod = "/example.v1.UserService/GetUser"

// newRegistryTestBackend starts a shared bytes backend which is closed when the test ends.
func newRegistryTestBackend(t *testing.T) *tools.GRPCBytesBackend {
	t.Helper()
	backend := tools.NewGRPCBytesBackend()
	t.Cleanup(backend.Close)
	return backend
}

// newRegistryTestClient returns a client for the registry tests. Its storage starts with a copy of storage.
func newRegistryTestClient(id string, userID string, ctx context.Context, storage map[string]any) *tools.TestClientMock {
	client := &tools.TestClientMock{
		IDFunc:     func() string { return id },
		UserIDFunc: func() string { return userID },
		ContextFunc: func() context.Context {
			if ctx == nil {
				return context.Background()
			}
			return ctx
		},
		TransportFunc: func() centrifuge.TransportInfo { return nil },
	}
	if len(storage) > 0 {
		copied, release := client.AcquireStorage()
		for key, value := range storage {
			copied[key] = value
		}
		release(copied)
	}
	return client
}

// registryTestStored returns the client storage value under key.
func registryTestStored(client *tools.TestClientMock, key string) (any, bool) {
	storage, release := client.AcquireStorage()
	defer release(storage)
	value, ok := storage[key]
	return value, ok
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
func newRegistryTestHandler(t *testing.T, cfg config.Config, registry *rpcregistry.Registry) (*Handler, *config.Container) {
	t.Helper()
	node := tools.NodeWithMemoryEngine()
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetRPCRegistry(registry)
	return h, cfgContainer
}

// startRegistryTestRegistry returns a registry with cfg applied, whose targets use the backend.
func startRegistryTestRegistry(t *testing.T, cfg configtypes.RPCRegistry, backend *tools.GRPCBytesBackend) *rpcregistry.Registry {
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
	require.Equal(t, connectToken, reply.Storage[clientstorage.KeyToken])
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
	stored, ok := registryTestStored(client, clientstorage.KeyToken)
	require.True(t, ok)
	require.Equal(t, refreshToken, stored)
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
	_, ok := reply.Storage[clientstorage.KeyToken]
	require.False(t, ok)
}

