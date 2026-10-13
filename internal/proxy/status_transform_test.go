package proxy

import (
	"errors"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
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
		{Status: "UNAUTHENTICATED", ToError: configtypes.TransformError{Code: 150, Message: "login again"}},
		{Status: "RESOURCE_EXHAUSTED", ToDisconnect: configtypes.TransformDisconnect{Code: 3600, Reason: "quota"}},
		{Status: "UNKNOWN", ToError: configtypes.TransformError{Code: 160}},
	}

	protocolError, protocolDisconnect, ok := transformGRPCStatus(codes.Unauthenticated, transforms)
	require.True(t, ok)
	require.Equal(t, uint32(150), protocolError.Code)
	require.Equal(t, "login again", protocolError.Message)
	require.Nil(t, protocolDisconnect)

	protocolError, protocolDisconnect, ok = transformGRPCStatus(codes.ResourceExhausted, transforms)
	require.True(t, ok)
	require.Nil(t, protocolError)
	require.Equal(t, uint32(3600), protocolDisconnect.Code)
	require.Equal(t, "quota", protocolDisconnect.Reason)

	_, _, ok = transformGRPCStatus(codes.Internal, transforms)
	require.False(t, ok)

	_, _, ok = transformGRPCStatus(codes.Unauthenticated, nil)
	require.False(t, ok)
}

func TestTransformGRPCStatusNamesMatchExactly(t *testing.T) {
	// Names are matched exactly; a lower case name never matches a status.
	transforms := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "unauthenticated", ToError: configtypes.TransformError{Code: 150}},
		{Status: "NOT_A_STATUS", ToError: configtypes.TransformError{Code: 151}},
	}
	_, _, ok := transformGRPCStatus(codes.Unauthenticated, transforms)
	require.False(t, ok)
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
