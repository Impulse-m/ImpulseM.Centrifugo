package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRPCRegistryResponseOverLimitUsesResponseTooLarge(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := registryTestRegistryConfig()
	reg.Errors.ResponseTooLarge = configtypes.RPCRegistryError{
		Code:    1001,
		Message: "response too large",
		AppCode: 500,
		AppType: "RESPONSE_TOO_LARGE",
	}
	registry := startRegistryTestRegistry(t, reg, backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.small", Data: []byte(tools.GRPCBytesBigPayload)}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(1001), protoErr.Code)
	require.Equal(t, "response too large", protoErr.Message)
	require.False(t, protoErr.Temporary)
	require.Equal(t, int32(500), protoErr.AppCode)
	require.Equal(t, "RESPONSE_TOO_LARGE", protoErr.AppType)
	require.NotContains(t, protoErr.Metadata, rpcRetryMetadataKey)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryTargetRefusalStaysRequestTooLarge(t *testing.T) {
	backend := newRegistryTestBackend(t)
	reg := registryTestRegistryConfig()
	reg.Errors.ResponseTooLarge = configtypes.RPCRegistryError{Code: 1001, Message: "response too large"}
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:           "users.refuse",
		GRPCMethod:       "/example.v1.UserService/Refuse",
		Target:           "backend",
		MaxRequestBytes:  8192,
		MaxResponseBytes: 4194304,
	})
	backend.FailMethod("/example.v1.UserService/Refuse", tools.GRPCBytesFailure{
		Err: status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
	})
	registry := startRegistryTestRegistry(t, reg, backend)

	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.refuse", Data: make([]byte, 5000)}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(106), protoErr.Code)
	require.Equal(t, "limit exceeded", protoErr.Message)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCRegistryApplicationErrorWithSizeTextKeepsAppFields(t *testing.T) {
	backend := newRegistryTestBackend(t)
	grpcMethod := registryStatusService + "Quota"
	backend.FailMethod(grpcMethod, tools.GRPCBytesFailure{
		Err:      status.Error(codes.ResourceExhausted, "quota: larger than max plan"),
		Trailers: metadata.Pairs("x-app-code", "429", "x-app-type", "QUOTA_EXCEEDED_17"),
	})

	cfg := registryErrorTestRegistry([]configtypes.RPCMethod{
		{Method: "users.get", GRPCMethod: grpcMethod, Target: "backend"},
	})
	cfg.AppError = configtypes.RPCRegistryAppError{CodeKey: "x-app-code", TypeKey: "x-app-type"}
	h := registryErrorTestHandler(t, backend, cfg)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Method: "users.get", Data: []byte("x")}, nil)
	protoErr := requireProtocolError(t, err)
	require.NotEqual(t, uint32(106), protoErr.Code)
	require.Equal(t, int32(429), protoErr.AppCode)
	require.Equal(t, "QUOTA_EXCEEDED_17", protoErr.AppType)
}
