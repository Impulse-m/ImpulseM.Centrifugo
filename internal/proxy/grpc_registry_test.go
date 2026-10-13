package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const registryTestMethod = "/example.v1.UserService/GetUser"

// Payloads which make the test backend slow or answer with a large reply.
const (
	registryTestSlowPayload = "slow"
	registryTestBigPayload  = "big"
	registryTestBigReply    = 1024
)

type registryTestCall struct {
	method string
	md     metadata.MD
}

// registryTestBackend is an in-process gRPC server. It answers every method with the raw request
// bytes and records the method and metadata of each call.
type registryTestBackend struct {
	listener *bufconn.Listener
	server   *grpc.Server

	mu    sync.Mutex
	calls []registryTestCall
}

func newRegistryTestBackend(t *testing.T) *registryTestBackend {
	t.Helper()
	b := &registryTestBackend{listener: bufconn.Listen(1 << 20)}
	b.server = grpc.NewServer(
		grpc.ForceServerCodec(bytesCodec{}),
		grpc.UnknownServiceHandler(b.handle),
	)
	go func() {
		_ = b.server.Serve(b.listener)
	}()
	t.Cleanup(b.server.Stop)
	return b
}

func (b *registryTestBackend) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	md, _ := metadata.FromIncomingContext(stream.Context())
	var in []byte
	err := stream.RecvMsg(&in)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.calls = append(b.calls, registryTestCall{method: method, md: md})
	b.mu.Unlock()

	if string(in) == registryTestSlowPayload {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	out := in
	if string(in) == registryTestBigPayload {
		out = make([]byte, registryTestBigReply)
	}
	return stream.SendMsg(&out)
}

func (b *registryTestBackend) recorded() []registryTestCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]registryTestCall(nil), b.calls...)
}

// conn returns a client connection to the backend, closed when the test ends.
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

// registryTestConfig returns a registry with one target and the methods the tests call.
func registryTestConfig() configtypes.RPCRegistry {
	return configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass:        configtypes.RPCAuthClassAuthenticated,
			Timeout:          configtypes.Duration(time.Second),
			MaxRequestBytes:  1 << 16,
			MaxResponseBytes: 1 << 16,
		},
		Targets: []configtypes.RPCTarget{
			{
				Name:            "backend",
				Endpoint:        "dns:///backend.example:9000",
				EmulatedHeaders: []string{"x-example-init", "x-example-shared"},
				GRPC: configtypes.ProxyCommonGRPC{
					StaticMetadata: configtypes.MapStringString{
						"x-example-static": "static",
						"x-example-shared": "static",
					},
				},
			},
		},
		Methods: []configtypes.RPCMethod{
			{
				Method:     "users.get",
				GRPCMethod: registryTestMethod,
				Target:     "backend",
				Headers:    []string{"x-example-export", "x-example-shared"},
			},
			{
				Method:     "users.slow",
				GRPCMethod: registryTestMethod,
				Target:     "backend",
				Timeout:    configtypes.Duration(50 * time.Millisecond),
			},
			{
				Method:           "users.small",
				GRPCMethod:       registryTestMethod,
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
	}
}

// startRegistryTest returns a registry with cfg applied, whose targets all use conn.
func startRegistryTest(t *testing.T, cfg configtypes.RPCRegistry, conn *grpc.ClientConn) *rpcregistry.Registry {
	t.Helper()
	dial := func(*rpcregistry.Target) (io.Closer, error) {
		return conn, nil
	}
	r, err := rpcregistry.Start(cfg, dial)
	require.NoError(t, err)
	return r
}

func TestRegistryCallPassesBytesThrough(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), backend.conn(t))

	payload := []byte{0x08, 0x96, 0x01, 0x00, 0xff, 0x7f}
	data, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.get"), RegistryCall{
		ClientID: "client-1",
		Data:     payload,
	})
	require.NoError(t, err)
	require.Equal(t, payload, data)

	calls := backend.recorded()
	require.Len(t, calls, 1)
	require.Equal(t, registryTestMethod, calls[0].method)
}

func TestRegistryCallTimeoutIsDeadlineExceeded(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), backend.conn(t))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.slow"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte(registryTestSlowPayload),
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

func TestRegistryCallResponseOverLimitIsReported(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), backend.conn(t))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.small"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte(registryTestBigPayload),
	})
	require.ErrorIs(t, err, ErrRPCResponseTooLarge)

	data, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.small"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte("ok"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), data)
}

func TestRegistryCallMetadataLaterSourcesWin(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), backend.conn(t))

	ctx := clientcontext.SetEmulatedHeadersToContext(context.Background(), map[string]string{
		"x-example-init":   "init",
		"x-example-other":  "not-listed",
		"x-example-shared": "connect",
	})
	_, _, err := CallRegistryMethod(ctx, r.Lookup("users.get"), RegistryCall{
		ClientID: "client-1",
		Token:    "token-1",
		Data:     []byte("payload"),
		Headers: map[string]string{
			"x-example-export": "export",
			"x-example-secret": "not-listed",
			"x-example-shared": "call",
		},
		MsgID: 7,
	})
	require.NoError(t, err)

	md := backend.recorded()[0].md
	require.Equal(t, []string{"static"}, md.Get("x-example-static"))
	require.Equal(t, []string{"init"}, md.Get("x-example-init"))
	require.Empty(t, md.Get("x-example-other"))
	require.Equal(t, []string{"call"}, md.Get("x-example-shared"))
	require.Equal(t, []string{"export"}, md.Get("x-example-export"))
	require.Empty(t, md.Get("x-example-secret"))
	require.Equal(t, []string{"client-1"}, md.Get("x-centrifugo-client"))
	require.Equal(t, []string{"7"}, md.Get("x-centrifugo-msg-id"))
	require.Equal(t, []string{"Bearer token-1"}, md.Get("authorization"))
}

func TestRegistryCallMetadataOmitsMsgIDAndAuthorization(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), backend.conn(t))

	// An anonymous method never gets the token, even on a connection which has one.
	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("help.config"), RegistryCall{
		ClientID: "client-1",
		Token:    "token-1",
	})
	require.NoError(t, err)

	// An authenticated method on a connection without a token gets no authorization.
	_, _, err = CallRegistryMethod(context.Background(), r.Lookup("users.get"), RegistryCall{
		ClientID: "client-2",
	})
	require.NoError(t, err)

	calls := backend.recorded()
	require.Len(t, calls, 2)
	require.Equal(t, []string{"client-1"}, calls[0].md.Get("x-centrifugo-client"))
	require.Empty(t, calls[0].md.Get("x-centrifugo-msg-id"))
	require.Empty(t, calls[0].md.Get("authorization"))
	require.Equal(t, []string{"client-2"}, calls[1].md.Get("x-centrifugo-client"))
	require.Empty(t, calls[1].md.Get("authorization"))
}

func TestRegistryRequestMetadataStripsReservedHeaders(t *testing.T) {
	m := &rpcregistry.Method{
		Name:       "users.get",
		GRPCMethod: registryTestMethod,
		Target:     &rpcregistry.Target{Config: configtypes.RPCTarget{Name: "backend"}},
		AuthClass:  rpcregistry.AuthClassAuthenticated,
		Headers:    []string{"authorization", "traceparent", "x-centrifugo-client", "x-example-export"},
	}
	md := registryRequestMetadata(context.Background(), m, RegistryCall{
		ClientID: "real-client",
		Headers: map[string]string{
			"authorization":       "client-value",
			"traceparent":         "client-value",
			"x-centrifugo-client": "client-value",
			"x-example-export":    "export",
		},
	})
	require.Equal(t, []string{"export"}, md.Get("x-example-export"))
	require.Equal(t, []string{"real-client"}, md.Get("x-centrifugo-client"))
	require.Empty(t, md.Get("authorization"))
	require.Empty(t, md.Get("traceparent"))
}

func TestDialRegistryTargetBuildsConnection(t *testing.T) {
	for _, policy := range []string{configtypes.RPCLoadBalancingRoundRobin, configtypes.RPCLoadBalancingPickFirst} {
		target := &rpcregistry.Target{Config: configtypes.RPCTarget{
			Name:          "backend",
			Endpoint:      "dns:///backend.example:9000",
			LoadBalancing: policy,
		}}
		closer, err := DialRegistryTarget(target)
		require.NoError(t, err)
		require.IsType(t, &grpc.ClientConn{}, closer)
		require.NoError(t, closer.Close())
	}
}

func TestBytesCodecPassesBytesAndCopiesOnUnmarshal(t *testing.T) {
	codec := bytesCodec{}
	require.Equal(t, "proto", codec.Name())

	raw := []byte("abc")
	marshaled, err := codec.Marshal(raw)
	require.NoError(t, err)
	require.Equal(t, raw, marshaled)
	marshaled, err = codec.Marshal(&raw)
	require.NoError(t, err)
	require.Equal(t, raw, marshaled)

	var out []byte
	src := []byte("xyz")
	require.NoError(t, codec.Unmarshal(src, &out))
	src[0] = '!'
	require.Equal(t, []byte("xyz"), out)

	_, err = codec.Marshal("text")
	require.Error(t, err)
	require.Error(t, codec.Unmarshal(src, raw))
}
