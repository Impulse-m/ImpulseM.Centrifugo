package proxy

import (
	"errors"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTransformHTTPStatusErrorMatchesSharedRules(t *testing.T) {
	transforms := []configtypes.HttpStatusToCodeTransform{
		{StatusCode: 403, ToError: configtypes.TransformError{Code: 103, Message: "denied", Temporary: true}},
		{StatusCode: 429, ToDisconnect: configtypes.TransformDisconnect{Code: 3500, Reason: "slow down"}},
	}

	protocolError, protocolDisconnect := transformHTTPStatusError(&statusCodeError{Code: 403}, transforms)
	require.Equal(t, uint32(103), protocolError.Code)
	require.Equal(t, "denied", protocolError.Message)
	require.True(t, protocolError.Temporary)
	require.Nil(t, protocolDisconnect)

	protocolError, protocolDisconnect = transformHTTPStatusError(&statusCodeError{Code: 429}, transforms)
	require.Nil(t, protocolError)
	require.Equal(t, uint32(3500), protocolDisconnect.Code)
	require.Equal(t, "slow down", protocolDisconnect.Reason)

	protocolError, protocolDisconnect = transformHTTPStatusError(&statusCodeError{Code: 500}, transforms)
	require.Nil(t, protocolError)
	require.Nil(t, protocolDisconnect)
}

func TestTransformHTTPStatusErrorSkipsEmptyTransformAndIgnoresOtherErrors(t *testing.T) {
	// A transform without a code does not stop the search: the next matching transform applies.
	transforms := []configtypes.HttpStatusToCodeTransform{
		{StatusCode: 400},
		{StatusCode: 400, ToError: configtypes.TransformError{Code: 111}},
	}
	protocolError, protocolDisconnect := transformHTTPStatusError(&statusCodeError{Code: 400}, transforms)
	require.Equal(t, uint32(111), protocolError.Code)
	require.Nil(t, protocolDisconnect)

	protocolError, protocolDisconnect = transformHTTPStatusError(errors.New("boom"), transforms)
	require.Nil(t, protocolError)
	require.Nil(t, protocolDisconnect)

	protocolError, protocolDisconnect = transformHTTPStatusError(&statusCodeError{Code: 400}, nil)
	require.Nil(t, protocolError)
	require.Nil(t, protocolDisconnect)
}

func TestTransformGRPCStatus(t *testing.T) {
	transforms := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "UNAUTHENTICATED", ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 150, Message: "login again"}}},
		{Status: "RESOURCE_EXHAUSTED", ToDisconnect: configtypes.TransformDisconnect{Code: 3600, Reason: "quota"}},
		{Status: "UNKNOWN", ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 160}}},
	}

	rule, ok := transformGRPCStatus(codes.Unauthenticated, transforms)
	require.True(t, ok)
	protocolError, protocolDisconnect := statusRuleResult(rule)
	require.Equal(t, uint32(150), protocolError.Code)
	require.Equal(t, "login again", protocolError.Message)
	require.Nil(t, protocolDisconnect)

	rule, ok = transformGRPCStatus(codes.ResourceExhausted, transforms)
	require.True(t, ok)
	protocolError, protocolDisconnect = statusRuleResult(rule)
	require.Nil(t, protocolError)
	require.Equal(t, uint32(3600), protocolDisconnect.Code)
	require.Equal(t, "quota", protocolDisconnect.Reason)

	_, ok = transformGRPCStatus(codes.Internal, transforms)
	require.False(t, ok)

	_, ok = transformGRPCStatus(codes.Unauthenticated, nil)
	require.False(t, ok)
}

func TestTransformGRPCStatusNamesMatchExactly(t *testing.T) {
	// Names are matched exactly; a lower case name never matches a status.
	transforms := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "unauthenticated", ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 150}}},
		{Status: "NOT_A_STATUS", ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 151}}},
	}
	_, ok := transformGRPCStatus(codes.Unauthenticated, transforms)
	require.False(t, ok)
}

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

func TestGRPCStatusByName(t *testing.T) {
	code, ok := grpcStatusByName("DATA_LOSS")
	require.True(t, ok)
	require.Equal(t, codes.DataLoss, code)

	code, ok = grpcStatusByName("OK")
	require.True(t, ok)
	require.Equal(t, codes.OK, code)

	_, ok = grpcStatusByName("data_loss")
	require.False(t, ok)
	_, ok = grpcStatusByName("15")
	require.False(t, ok)
}

func TestDefaultGRPCStatusError(t *testing.T) {
	tests := []struct {
		code      codes.Code
		wantCode  uint32
		temporary bool
	}{
		{codes.Unauthenticated, 101, false},
		{codes.PermissionDenied, 103, false},
		{codes.ResourceExhausted, 111, true},
		{codes.Unavailable, 100, true},
		{codes.DeadlineExceeded, 100, true},
		{codes.Internal, 100, false},
		{codes.Unknown, 100, false},
		{codes.Unimplemented, 100, false},
		{codes.DataLoss, 100, false},
		{codes.InvalidArgument, 400, false},
		{codes.FailedPrecondition, 400, false},
		{codes.NotFound, 400, false},
		{codes.AlreadyExists, 400, false},
		{codes.Aborted, 400, false},
		{codes.OutOfRange, 400, false},
	}
	for _, tt := range tests {
		protocolError := defaultGRPCStatusError(tt.code, "status text")
		require.NotNil(t, protocolError, tt.code.String())
		require.Equal(t, tt.wantCode, protocolError.Code, tt.code.String())
		require.Equal(t, tt.temporary, protocolError.Temporary, tt.code.String())
		require.Equal(t, "status text", protocolError.Message, tt.code.String())
	}
}

func TestDefaultGRPCStatusErrorNilForOKAndCanceled(t *testing.T) {
	// The caller maps a cancelled call from the registry error config, not from the defaults.
	require.Nil(t, defaultGRPCStatusError(codes.OK, ""))
	require.Nil(t, defaultGRPCStatusError(codes.Canceled, "context canceled"))
}

func TestDefaultGRPCStatusErrorMessageCap(t *testing.T) {
	long := strings.Repeat("a", 300)
	protocolError := defaultGRPCStatusError(codes.Internal, long)
	require.Len(t, protocolError.Message, grpcErrorMessageMaxLength)

	// A multi-byte rune straddling the limit is dropped whole.
	multi := strings.Repeat("a", 255) + "é"
	protocolError = defaultGRPCStatusError(codes.Internal, multi)
	require.Equal(t, strings.Repeat("a", 255), protocolError.Message)

	exact := strings.Repeat("b", grpcErrorMessageMaxLength)
	require.Equal(t, exact, capGRPCErrorMessage(exact))
	require.Equal(t, "short", capGRPCErrorMessage("short"))
}

func TestHTTPAndGRPCShareMatcher(t *testing.T) {
	// Both kinds go through matchStatusRule: the same rule shape gives the same result.
	httpRules := httpStatusRules(configtypes.HttpStatusToCodeTransforms{
		{StatusCode: 401, ToError: configtypes.TransformError{Code: 101}},
	})
	protocolError, _, ok := matchStatusRule(401, httpRules)
	require.True(t, ok)
	require.Equal(t, uint32(101), protocolError.Code)

}

func TestMapGRPCRegistryErrorBackendCanceledUsesInternalClass(t *testing.T) {
	protocolError, disconnect := MapGRPCRegistryError(
		status.Error(codes.Canceled, "backend cancelled"),
		nil,
		nil,
		configtypes.RPCRegistryAppError{},
		nil,
	)
	require.Nil(t, disconnect)
	require.NotNil(t, protocolError)
	require.Equal(t, uint32(100), protocolError.Code)
	require.Equal(t, "backend cancelled", protocolError.Message)
	require.False(t, protocolError.Temporary)
}
