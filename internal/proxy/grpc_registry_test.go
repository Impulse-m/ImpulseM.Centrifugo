package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const registryTestMethod = "/example.v1.UserService/GetUser"

// newRegistryTestBackend starts a shared bytes backend which is closed when the test ends.
func newRegistryTestBackend(t *testing.T) *tools.GRPCBytesBackend {
	t.Helper()
	backend := tools.NewGRPCBytesBackend()
	t.Cleanup(backend.Close)
	return backend
}

// registryTestConn returns a client connection to backend.
func registryTestConn(t *testing.T, backend *tools.GRPCBytesBackend) *grpc.ClientConn {
	t.Helper()
	conn, err := backend.Conn()
	require.NoError(t, err)
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
				Method:     "system.config",
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
	r := startRegistryTest(t, registryTestConfig(), registryTestConn(t, backend))

	payload := []byte{0x08, 0x96, 0x01, 0x00, 0xff, 0x7f}
	data, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.get"), RegistryCall{
		ClientID: "client-1",
		Data:     payload,
	})
	require.NoError(t, err)
	require.Equal(t, payload, data)

	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, registryTestMethod, calls[0].Method)
}

func TestRegistryCallTimeoutIsDeadlineExceeded(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), registryTestConn(t, backend))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.slow"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte(tools.GRPCBytesSlowPayload),
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

func TestRegistryCallResponseOverLimitIsReported(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), registryTestConn(t, backend))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.small"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte(tools.GRPCBytesBigPayload),
	})
	require.ErrorIs(t, err, ErrRPCResponseTooLarge)

	data, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.small"), RegistryCall{
		ClientID: "client-1",
		Data:     []byte("ok"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), data)
}

func TestRegistryMessageTooLarge(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		requestLen int
		bound      int
		compressed bool
		tooLarge   bool
		reply      bool
	}{
		{
			name:       "reply over the bound",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound after decompression",
			err:        status.Error(codes.ResourceExhausted, "grpc: message after decompression larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound without a size",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message after decompression larger than max 1024"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "target limit equal to the bound refuses an echo of the request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
			requestLen: 5000,
			bound:      4194304,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "reply over a large bound",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
			requestLen: 100,
			bound:      4194304,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound on a 32-bit build",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 9223372036854775807)"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "echo whose reply bound is below its request is a refused request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (20480 vs. 16384)"),
			requestLen: 20480,
			bound:      16384,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "compressed target is taken as a refused request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			compressed: true,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "quota is not a message size",
			err:        status.Error(codes.ResourceExhausted, "quota exceeded"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "other code with the size text",
			err:        status.Error(codes.Unavailable, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "not a status",
			err:        errors.New("grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tooLarge, reply := registryMessageTooLarge(tt.err, tt.requestLen, tt.bound, tt.compressed)
			require.Equal(t, tt.tooLarge, tooLarge)
			require.Equal(t, tt.reply, reply)
		})
	}
}

func TestRegistryCallTargetRefusingRequestIsReported(t *testing.T) {
	backend := newRegistryTestBackend(t)
	cfg := registryTestConfig()
	cfg.Methods = append(cfg.Methods, configtypes.RPCMethod{
		Method:           "users.refuse",
		GRPCMethod:       "/example.v1.UserService/Refuse",
		Target:           "backend",
		MaxRequestBytes:  8192,
		MaxResponseBytes: 4194304,
	})
	backend.FailMethod("/example.v1.UserService/Refuse", tools.GRPCBytesFailure{
		Err: status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
	})
	r := startRegistryTest(t, cfg, registryTestConn(t, backend))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.refuse"), RegistryCall{
		ClientID: "client-1",
		Data:     make([]byte, 5000),
	})
	require.ErrorIs(t, err, ErrRPCTargetRequestTooLarge)
	require.NotErrorIs(t, err, ErrRPCResponseTooLarge)
}

func TestRegistryCallMetadataLaterSourcesWin(t *testing.T) {
	backend := newRegistryTestBackend(t)
	r := startRegistryTest(t, registryTestConfig(), registryTestConn(t, backend))

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

	md := backend.Calls()[0].Metadata
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
	r := startRegistryTest(t, registryTestConfig(), registryTestConn(t, backend))

	// An anonymous method never gets the token, even on a connection which has one.
	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("system.config"), RegistryCall{
		ClientID: "client-1",
		Token:    "token-1",
	})
	require.NoError(t, err)

	// An authenticated method on a connection without a token gets no authorization.
	_, _, err = CallRegistryMethod(context.Background(), r.Lookup("users.get"), RegistryCall{
		ClientID: "client-2",
	})
	require.NoError(t, err)

	calls := backend.Calls()
	require.Len(t, calls, 2)
	require.Equal(t, []string{"client-1"}, calls[0].Metadata.Get("x-centrifugo-client"))
	require.Empty(t, calls[0].Metadata.Get("x-centrifugo-msg-id"))
	require.Empty(t, calls[0].Metadata.Get("authorization"))
	require.Equal(t, []string{"client-2"}, calls[1].Metadata.Get("x-centrifugo-client"))
	require.Empty(t, calls[1].Metadata.Get("authorization"))
}

// A validated method never lists a reserved header, so client values of reserved names are not
// forwarded at all.
func TestRegistryRequestMetadataDropsUnlistedReservedHeaders(t *testing.T) {
	m := &rpcregistry.Method{
		Name:       "users.get",
		GRPCMethod: registryTestMethod,
		Target:     &rpcregistry.Target{Config: configtypes.RPCTarget{Name: "backend"}},
		AuthClass:  rpcregistry.AuthClassAuthenticated,
		Headers:    []string{"x-example-export"},
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

// The reserved values are written after the client headers, so they win even when a method
// lists reserved names, which validation would reject.
func TestRegistryRequestMetadataServerValuesWinOverListedReservedHeaders(t *testing.T) {
	m := &rpcregistry.Method{
		Name:       "users.get",
		GRPCMethod: registryTestMethod,
		Target:     &rpcregistry.Target{Config: configtypes.RPCTarget{Name: "backend"}},
		AuthClass:  rpcregistry.AuthClassAuthenticated,
		Headers:    []string{"authorization", "x-centrifugo-client", "x-centrifugo-msg-id"},
	}
	md := registryRequestMetadata(context.Background(), m, RegistryCall{
		ClientID: "real-client",
		Token:    "good",
		MsgID:    9,
		Headers: map[string]string{
			"authorization":       "client-value",
			"x-centrifugo-client": "client-value",
			"x-centrifugo-msg-id": "client-value",
		},
	})
	require.Equal(t, []string{"Bearer good"}, md.Get("authorization"))
	require.Equal(t, []string{"real-client"}, md.Get("x-centrifugo-client"))
	require.Equal(t, []string{"9"}, md.Get("x-centrifugo-msg-id"))
}

// registryForwardedTarget is a method whose target lets x-forwarded-for through as an emulated
// header. Validation rejects that name, so the test builds the target directly to put a
// client-sent value in front of the registry.
func registryForwardedTarget() *rpcregistry.Method {
	return &rpcregistry.Method{
		Name:       "users.get",
		GRPCMethod: registryTestMethod,
		Target: &rpcregistry.Target{Config: configtypes.RPCTarget{
			Name:            "backend",
			EmulatedHeaders: []string{"x-forwarded-for"},
		}},
		AuthClass: rpcregistry.AuthClassAuthenticated,
	}
}

func TestRegistryRequestMetadataOverwritesClientForwardedFor(t *testing.T) {
	ctx := clientcontext.SetEmulatedHeadersToContext(context.Background(), map[string]string{"x-forwarded-for": "9.9.9.9"})
	ctx = clientaddr.WithPeer(peer.NewContext(ctx, &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443},
	}))
	md := registryRequestMetadata(ctx, registryForwardedTarget(), RegistryCall{ClientID: "real-client"})
	require.Equal(t, []string{"2001:db8::1"}, md.Get("x-forwarded-for"))
}

func TestRegistryRequestMetadataOmitsUnresolvedForwardedFor(t *testing.T) {
	// No resolved address: the header is omitted, and a client-sent value is dropped with it.
	ctx := clientcontext.SetEmulatedHeadersToContext(context.Background(), map[string]string{"x-forwarded-for": "9.9.9.9"})
	md := registryRequestMetadata(ctx, registryForwardedTarget(), RegistryCall{ClientID: "real-client"})
	require.Empty(t, md.Get("x-forwarded-for"))

	md = registryRequestMetadata(context.Background(), registryForwardedTarget(), RegistryCall{ClientID: "real-client"})
	require.Empty(t, md.Get("x-forwarded-for"))
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
