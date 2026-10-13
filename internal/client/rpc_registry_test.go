package client

import (
	"context"
	"io"
	"net"
	"sync"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const registryTestBackendMethod = "/example.v1.UserService/GetUser"

// fakeRegistryClient is a client for the registry tests. Storage is shared with the test.
type fakeRegistryClient struct {
	id      string
	userID  string
	ctx     context.Context
	storage map[string]any
}

func (c *fakeRegistryClient) ID() string {
	return c.id
}

func (c *fakeRegistryClient) UserID() string {
	return c.userID
}

func (c *fakeRegistryClient) IsSubscribed(string) bool {
	return false
}

func (c *fakeRegistryClient) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *fakeRegistryClient) Transport() centrifuge.TransportInfo {
	return nil
}

func (c *fakeRegistryClient) AcquireStorage() (map[string]any, func(map[string]any)) {
	return c.storage, func(map[string]any) {}
}

func (c *fakeRegistryClient) Unsubscribe(string, ...centrifuge.Unsubscribe) {}

func (c *fakeRegistryClient) WritePublication(string, *centrifuge.Publication, centrifuge.StreamPosition) error {
	return nil
}

// registryTestBackend answers every call with the raw request bytes and records the metadata of each call.
type registryTestBackend struct {
	listener *bufconn.Listener
	server   *grpc.Server

	mu       sync.Mutex
	received []metadata.MD
}

func newRegistryTestBackend(t *testing.T) *registryTestBackend {
	t.Helper()
	b := &registryTestBackend{listener: bufconn.Listen(1 << 20)}
	b.server = grpc.NewServer(
		grpc.ForceServerCodec(clientBytesCodec{}),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			var in []byte
			err := stream.RecvMsg(&in)
			if err != nil {
				return err
			}
			b.mu.Lock()
			b.received = append(b.received, md)
			b.mu.Unlock()
			out := in
			return stream.SendMsg(&out)
		}),
	)
	go func() {
		_ = b.server.Serve(b.listener)
	}()
	t.Cleanup(b.server.Stop)
	return b
}

func (b *registryTestBackend) calls() []metadata.MD {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]metadata.MD(nil), b.received...)
}

func (b *registryTestBackend) conn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///registry-test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return b.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return conn
}

// clientBytesCodec passes message bytes through. It mirrors the codec of the proxy package, which is unexported.
type clientBytesCodec struct{}

func (clientBytesCodec) Name() string {
	return "proto"
}

func (clientBytesCodec) Marshal(v any) ([]byte, error) {
	if raw, ok := v.(*[]byte); ok {
		return *raw, nil
	}
	return v.([]byte), nil
}

func (clientBytesCodec) Unmarshal(data []byte, v any) error {
	raw := v.(*[]byte)
	*raw = append((*raw)[:0], data...)
	return nil
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
func startRegistryTestRegistry(t *testing.T, cfg configtypes.RPCRegistry, backend *registryTestBackend) *rpcregistry.Registry {
	t.Helper()
	conn := backend.conn(t)
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
	client := &fakeRegistryClient{id: "c1", userID: "42", storage: map[string]any{}}
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
	client := &fakeRegistryClient{id: "c1", userID: "42", storage: map[string]any{}}

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

	anonymous := &fakeRegistryClient{id: "c1", storage: map[string]any{}}
	authenticated := &fakeRegistryClient{id: "c2", userID: "42", storage: map[string]any{}}

	// An anonymous connection cannot call an authenticated method.
	_, err := h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(103), protoErr.Code)

	// The request size gate is checked before the call.
	_, err = h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "users.get", Data: make([]byte, 17)}, nil)
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(106), protoErr.Code)
	require.Empty(t, backend.calls())

	// An anonymous method is callable by everyone, and the raw bytes come back.
	reply, err := h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "help.config", Data: []byte("raw")}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("raw"), reply.Data)
	require.Len(t, backend.calls(), 1)
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
	client := &fakeRegistryClient{id: "c1", userID: "42", ctx: reply.Context, storage: reply.Storage}
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	require.NoError(t, err)
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "help.config", Data: []byte("x")}, nil)
	require.NoError(t, err)

	calls := backend.calls()
	require.Len(t, calls, 2)
	require.Equal(t, []string{"Bearer " + connectToken}, calls[0].Get("authorization"))
	require.Equal(t, []string{"init"}, calls[0].Get("x-example-init"))
	require.Empty(t, calls[1].Get("authorization"))

	// A refresh replaces the stored token, so later calls carry the new one.
	refreshToken := getConnTokenHS("42", time.Now().Add(time.Hour).Unix())
	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{Token: refreshToken}, nil)
	require.NoError(t, err)
	require.Equal(t, refreshToken, client.storage[clientstorage.KeyToken])
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

func TestRegistryCallErrorMapping(t *testing.T) {
	errs := registryTestRegistryConfig().Errors

	temporary := []codes.Code{codes.Unavailable, codes.DeadlineExceeded}
	for _, code := range temporary {
		var protoErr *centrifuge.Error
		err := registryCallError(status.Error(code, "backend said so"), errs)
		require.ErrorAs(t, err, &protoErr)
		require.Equal(t, uint32(100), protoErr.Code)
		require.Equal(t, "backend said so", protoErr.Message)
		require.True(t, protoErr.Temporary)
	}

	var protoErr *centrifuge.Error
	err := registryCallError(status.Error(codes.Internal, "boom"), errs)
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(100), protoErr.Code)
	require.False(t, protoErr.Temporary)

	err = registryCallError(proxy.ErrRPCResponseTooLarge, errs)
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, uint32(106), protoErr.Code)
}
