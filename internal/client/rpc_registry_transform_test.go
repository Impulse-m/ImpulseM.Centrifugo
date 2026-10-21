package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCRegistryTransformAppFieldsReachClient(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "Unavailable"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{Err: status.Error(codes.Unavailable, "connection refused")})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.get", GRPCMethod: grpcMethod, Target: "backend"},
	})
	cfg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
		{
			Status: "UNAVAILABLE",
			ToError: configtypes.RPCTransformError{
				TransformError: configtypes.TransformError{Code: 100, Message: "backend unavailable", Temporary: true},
				AppCode:        503,
				AppType:        "BACKEND_UNAVAILABLE",
			},
		},
	}
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)
	errorsBefore := testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "BACKEND_UNAVAILABLE"))

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(100), protoErr.Code)
	require.Equal(t, "backend unavailable", protoErr.Message)
	require.True(t, protoErr.Temporary)
	require.Equal(t, int32(503), protoErr.AppCode)
	require.Equal(t, "BACKEND_UNAVAILABLE", protoErr.AppType)
	require.Equal(t, errorsBefore+1, testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "BACKEND_UNAVAILABLE")))
}
