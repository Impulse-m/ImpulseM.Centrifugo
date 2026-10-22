package proxy

import (
	"errors"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMapGRPCRegistryErrorTransformAppFields(t *testing.T) {
	transforms := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "UNAVAILABLE", ToError: configtypes.RPCTransformError{
			TransformError: configtypes.TransformError{Code: 100, Message: "backend unavailable", Temporary: true},
			AppCode:        503,
			AppType:        "BACKEND_UNAVAILABLE",
		}},
		{Status: "RESOURCE_EXHAUSTED", ToDisconnect: configtypes.TransformDisconnect{Code: 3600, Reason: "quota"}},
		{Status: "UNAVAILABLE", ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 101}}},
	}
	trailersWithApp := metadata.Pairs("app-code", "429", "app-type", "QUOTA_EXCEEDED_17")
	appKeys := configtypes.RPCRegistryAppError{CodeKey: "app-code", TypeKey: "app-type"}

	tests := []struct {
		name        string
		err         error
		trailers    metadata.MD
		wantCode    uint32
		wantMessage string
		wantTemp    bool
		wantAppCode int32
		wantAppType string
		wantDisconn uint32
	}{
		{
			name:        "rule fills missing app fields",
			err:         status.Error(codes.Unavailable, "connection refused"),
			wantCode:    100,
			wantMessage: "backend unavailable",
			wantTemp:    true,
			wantAppCode: 503,
			wantAppType: "BACKEND_UNAVAILABLE",
		},
		{
			name:        "trailers win over the rule",
			err:         status.Error(codes.Unavailable, "connection refused"),
			trailers:    trailersWithApp,
			wantCode:    100,
			wantMessage: "backend unavailable",
			wantTemp:    true,
			wantAppCode: 429,
			wantAppType: "QUOTA_EXCEEDED_17",
		},
		{
			name:        "no rule keeps the default table",
			err:         status.Error(codes.DeadlineExceeded, "deadline"),
			wantCode:    100,
			wantMessage: "deadline",
			wantTemp:    true,
			wantAppCode: 500,
		},
		{
			name:        "disconnect rule",
			err:         status.Error(codes.ResourceExhausted, "quota"),
			wantDisconn: 3600,
		},
		{
			name:        "first of two matching rules wins with its app fields",
			err:         status.Error(codes.Unavailable, "connection refused"),
			wantCode:    100,
			wantMessage: "backend unavailable",
			wantTemp:    true,
			wantAppCode: 503,
			wantAppType: "BACKEND_UNAVAILABLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocolError, disconnect := MapGRPCRegistryError(tt.err, tt.trailers, transforms, appKeys, nil)
			if tt.wantDisconn != 0 {
				require.Nil(t, protocolError)
				require.NotNil(t, disconnect)
				require.Equal(t, tt.wantDisconn, disconnect.Code)
				return
			}
			require.Nil(t, disconnect)
			require.Equal(t, tt.wantCode, protocolError.Code)
			require.Equal(t, tt.wantMessage, protocolError.Message)
			require.Equal(t, tt.wantTemp, protocolError.Temporary)
			require.Equal(t, tt.wantAppCode, protocolError.AppCode)
			require.Equal(t, tt.wantAppType, protocolError.AppType)
		})
	}
}

// TestRawGoErrorMessageNotForwarded verifies that the text of a plain Go error, which may name internal
// addresses, does not reach the client.
func TestRawGoErrorMessageNotForwarded(t *testing.T) {
	appKeys := configtypes.RPCRegistryAppError{CodeKey: "app-code", TypeKey: "app-type"}
	err := errors.New("dial tcp 10.0.0.5:9000: connect: connection refused")

	protocolError, disconnect := MapGRPCRegistryError(err, nil, nil, appKeys, nil)
	require.Nil(t, disconnect)
	require.NotNil(t, protocolError)
	require.NotContains(t, protocolError.Message, "10.0.0.5")
}
