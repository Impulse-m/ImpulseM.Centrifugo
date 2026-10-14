package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const registryStatusService = "/example.v1.StatusService/"

// registryErrorTestRegistry returns a registry config with the given methods, all calling the backend
// target with the default table and the default error protocol errors.
func registryErrorTestRegistry(methods []configtypes.RPCMethod) configtypes.RPCRegistry {
	return configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass:        configtypes.RPCAuthClassAuthenticated,
			Timeout:          configtypes.Duration(time.Second),
			MaxRequestBytes:  64,
			MaxResponseBytes: 1 << 16,
		},
		Targets: []configtypes.RPCTarget{
			{
				Name:     "backend",
				Endpoint: "dns:///backend.example:9000",
			},
		},
		Methods: methods,
		Errors: configtypes.RPCRegistryErrors{
			UnknownMethod:   configtypes.RPCRegistryError{Code: 104, Message: "method not found"},
			AuthClass:       configtypes.RPCRegistryError{Code: 103, Message: "permission denied"},
			RequestTooLarge: configtypes.RPCRegistryError{Code: 106, Message: "limit exceeded"},
			Cancelled:       configtypes.RPCRegistryError{Code: 107, Message: "cancelled"},
		},
	}
}

// registryErrorTestHandler starts a registry with the config and the backend, and returns a handler
// which uses it.
func registryErrorTestHandler(t *testing.T, backend *tools.GRPCBytesBackend, registry configtypes.RPCRegistry) *Handler {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registry
	started := startRegistryTestRegistry(t, registry, backend)
	h, _ := newRegistryTestHandler(t, cfg, started)
	return h
}

// requireProtocolError asserts err is a protocol error and returns it.
func requireProtocolError(t *testing.T, err error) *centrifuge.Error {
	t.Helper()
	var protoErr *centrifuge.Error
	require.ErrorAs(t, err, &protoErr)
	return protoErr
}

func TestRegistryDefaultStatusTableEndToEnd(t *testing.T) {
	type row struct {
		status    codes.Code
		code      uint32
		temporary bool
		appCode   int32
	}
	rows := []row{
		{codes.Unauthenticated, 101, false, 401},
		{codes.PermissionDenied, 103, false, 403},
		{codes.ResourceExhausted, 111, true, 420},
		{codes.Unavailable, 100, true, 500},
		{codes.DeadlineExceeded, 100, true, 500},
		{codes.Internal, 100, false, 500},
		{codes.Unknown, 100, false, 500},
		{codes.Unimplemented, 100, false, 500},
		{codes.DataLoss, 100, false, 500},
		{codes.InvalidArgument, 400, false, 400},
		{codes.FailedPrecondition, 400, false, 500},
		{codes.NotFound, 400, false, 500},
		{codes.AlreadyExists, 400, false, 500},
		{codes.Aborted, 400, false, 500},
		{codes.OutOfRange, 400, false, 500},
	}

	backend := newRegistryTestBackend(t)
	methods := make([]configtypes.RPCMethod, 0, len(rows))
	for _, r := range rows {
		grpcMethod := registryStatusService + r.status.String()
		methods = append(methods, configtypes.RPCMethod{
			Method:     "status." + r.status.String(),
			GRPCMethod: grpcMethod,
			Target:     "backend",
		})
		backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{
			Err: status.Error(r.status, "backend message"),
		})
	}
	h := registryErrorTestHandler(t, backend, registryErrorTestRegistry(methods))
	client := newRegistryTestClient("c1", "42", nil, nil)

	for _, r := range rows {
		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "status." + r.status.String(), Data: []byte("x")}, nil)
		protoErr := requireProtocolError(t, err)
		require.Equal(t, r.code, protoErr.Code, r.status.String())
		require.Equal(t, "backend message", protoErr.Message, r.status.String())
		require.Equal(t, r.temporary, protoErr.Temporary, r.status.String())
		require.Equal(t, r.appCode, protoErr.AppCode, r.status.String())
		require.Empty(t, protoErr.AppType, r.status.String())
		require.Nil(t, protoErr.Metadata, r.status.String())
	}
}

func TestRegistryStatusTransformOverridesDefault(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "Unauthenticated"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{Err: status.Error(codes.Unauthenticated, "no token")})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.get", GRPCMethod: grpcMethod, Target: "backend"},
	})
	cfg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
		{
			Status:  "UNAUTHENTICATED",
			ToError: configtypes.TransformError{Code: 150, Message: "custom unauthorized", Temporary: true},
		},
	}
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(150), protoErr.Code)
	require.Equal(t, "custom unauthorized", protoErr.Message)
	require.True(t, protoErr.Temporary)
	// The application fields still come from the default of the status.
	require.Equal(t, int32(401), protoErr.AppCode)
}

func TestRegistryStatusTransformToDisconnect(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "ResourceExhausted"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{Err: status.Error(codes.ResourceExhausted, "slow down")})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.get", GRPCMethod: grpcMethod, Target: "backend"},
	})
	cfg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
		{
			Status:       "RESOURCE_EXHAUSTED",
			ToDisconnect: configtypes.TransformDisconnect{Code: 3500, Reason: "too many requests"},
		},
	}
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	var disconnect *centrifuge.Disconnect
	require.ErrorAs(t, err, &disconnect)
	require.Equal(t, uint32(3500), disconnect.Code)
	require.Equal(t, "too many requests", disconnect.Reason)
}

func TestRegistryAppErrorFromTrailersWithCustomKeys(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "FailedPrecondition"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{
		Err: status.Error(codes.FailedPrecondition, "ignored"),
		Trailers: metadata.Pairs(
			"x-app-code", "1234",
			"x-app-type", "CUSTOM_TYPE",
			"x-detail", "more",
			"x-secret", "hidden",
		),
	})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{
			Method:        "users.get",
			GRPCMethod:    grpcMethod,
			Target:        "backend",
			ErrorMetadata: []string{"x-detail"},
		},
		{
			Method:     "users.plain",
			GRPCMethod: grpcMethod,
			Target:     "backend",
		},
	})
	cfg.AppError = configtypes.RPCRegistryAppError{CodeKey: "x-app-code", TypeKey: "x-app-type"}
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(400), protoErr.Code)
	require.Equal(t, int32(1234), protoErr.AppCode)
	require.Equal(t, "CUSTOM_TYPE", protoErr.AppType)
	// Only the trailer named in error_metadata is copied, the other one is not passed.
	require.Equal(t, map[string]string{"x-detail": "more"}, protoErr.Metadata)

	// A method without error_metadata copies no trailer at all.
	_, err = h.OnRPC(client, centrifuge.RPCEvent{Method: "users.plain", Data: []byte("x")}, nil)
	protoErr = requireProtocolError(t, err)
	require.Equal(t, int32(1234), protoErr.AppCode)
	require.Nil(t, protoErr.Metadata)
}

func TestRegistryAppTypeFallsBackToStatusMessage(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "ResourceExhaustedMessage"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{Err: status.Error(codes.ResourceExhausted, "QUOTA_EXCEEDED_17")})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.get", GRPCMethod: grpcMethod, Target: "backend"},
	})
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(111), protoErr.Code)
	require.Equal(t, "QUOTA_EXCEEDED_17", protoErr.Message)
	require.Equal(t, "QUOTA_EXCEEDED_17", protoErr.AppType)
	require.Equal(t, int32(420), protoErr.AppCode)
}

func TestRegistryCancelledCallIsCancelledError(t *testing.T) {
	backend := newRegistryTestBackend(t)
	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.slow", GRPCMethod: registryStatusService + "Slow", Target: "backend"},
	})
	h := registryErrorTestHandler(t, backend, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newRegistryTestClient("c1", "42", ctx, nil)

	errCh := make(chan error, 1)
	go func() {
		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.slow", Data: []byte(tools.GRPCBytesSlowPayload)}, nil)
		errCh <- err
	}()
	require.Eventually(t, func() bool { return len(backend.Calls()) == 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()

	err := <-errCh
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(107), protoErr.Code)
	require.Equal(t, "cancelled", protoErr.Message)
}

func TestRegistryCallMetricsLabels(t *testing.T) {
	backend := newRegistryTestBackend(t)
	quotaMethod := registryStatusService + "Quota"
	backend.FailMethod(quotaMethod, tools.GRPCBytesFailure{Err: status.Error(codes.ResourceExhausted, "QUOTA_EXCEEDED_17")})
	timeoutMethod := registryStatusService + "Slow"

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "metrics.ok", GRPCMethod: registryStatusService + "Echo", Target: "backend"},
		{Method: "metrics.quota", GRPCMethod: quotaMethod, Target: "backend"},
		{Method: "metrics.timeout", GRPCMethod: timeoutMethod, Target: "backend", Timeout: configtypes.Duration(50 * time.Millisecond)},
		{Method: "metrics.authed", GRPCMethod: registryStatusService + "Echo", Target: "backend"},
	})
	h := registryErrorTestHandler(t, backend, cfg)
	authenticated := newRegistryTestClient("c1", "42", nil, nil)
	anonymous := newRegistryTestClient("c2", "", nil, nil)

	callsBefore := func(method string, result string) float64 {
		return testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues(method, result))
	}
	errorsBefore := func(method string, appType string) float64 {
		return testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues(method, appType))
	}
	proxyErrorsBefore := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "rpc", "backend"))

	// An unknown method counts under the unknown label.
	unknownCalls := callsBefore("unknown", "unknown")
	unknownErrors := errorsBefore("unknown", "none")
	_, err := h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "metrics.missing"}, nil)
	requireProtocolError(t, err)
	require.Equal(t, unknownCalls+1, callsBefore("unknown", "unknown"))
	require.Equal(t, unknownErrors+1, errorsBefore("unknown", "none"))

	// An anonymous connection calling an authenticated method is denied by the class gate.
	deniedCalls := callsBefore("metrics.authed", "denied")
	_, err = h.OnRPC(anonymous, centrifuge.RPCEvent{Method: "metrics.authed", Data: []byte("x")}, nil)
	requireProtocolError(t, err)
	require.Equal(t, deniedCalls+1, callsBefore("metrics.authed", "denied"))

	okCalls := callsBefore("metrics.ok", "ok")
	_, err = h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "metrics.ok", Data: []byte("x")}, nil)
	require.NoError(t, err)
	require.Equal(t, okCalls+1, callsBefore("metrics.ok", "ok"))

	// A failed backend call counts as error, with the normalised app type, and as a proxy error.
	quotaCalls := callsBefore("metrics.quota", "error")
	quotaErrors := errorsBefore("metrics.quota", "QUOTA_EXCEEDED_X")
	_, err = h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "metrics.quota", Data: []byte("x")}, nil)
	requireProtocolError(t, err)
	require.Equal(t, quotaCalls+1, callsBefore("metrics.quota", "error"))
	require.Equal(t, quotaErrors+1, errorsBefore("metrics.quota", "QUOTA_EXCEEDED_X"))
	require.Equal(t, proxyErrorsBefore+1, testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "rpc", "backend")))

	// A call over its timeout counts as timeout.
	timeoutCalls := callsBefore("metrics.timeout", "timeout")
	_, err = h.OnRPC(authenticated, centrifuge.RPCEvent{Method: "metrics.timeout", Data: []byte(tools.GRPCBytesSlowPayload)}, nil)
	requireProtocolError(t, err)
	require.Equal(t, timeoutCalls+1, callsBefore("metrics.timeout", "timeout"))
}

func TestRegistryCancelledCallIsNotAProxyError(t *testing.T) {
	backend := newRegistryTestBackend(t)
	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "metrics.cancel", GRPCMethod: registryStatusService + "Slow", Target: "backend"},
	})
	h := registryErrorTestHandler(t, backend, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newRegistryTestClient("c1", "42", ctx, nil)

	cancelledCalls := testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("metrics.cancel", "cancelled"))
	proxyErrors := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "rpc", "backend"))

	errCh := make(chan error, 1)
	go func() {
		_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "metrics.cancel", Data: []byte(tools.GRPCBytesSlowPayload)}, nil)
		errCh <- err
	}()
	require.Eventually(t, func() bool { return len(backend.Calls()) == 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	requireProtocolError(t, <-errCh)

	require.Equal(t, cancelledCalls+1, testutil.ToFloat64(metrics.RPCRegistryCallsTotal.WithLabelValues("metrics.cancel", "cancelled")))
	require.Equal(t, proxyErrors, testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "rpc", "backend")))
}
