package proxy

import (
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var testAppErrorKeys = configtypes.RPCRegistryAppError{
	CodeKey: "app-code",
	TypeKey: "app-type",
}

func TestExtractGRPCAppErrorCodeFromTrailer(t *testing.T) {
	trailers := metadata.Pairs("app-code", "17")
	result := extractGRPCAppError(trailers, codes.FailedPrecondition, "failed", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, int32(17), result.AppCode)
}

func TestExtractGRPCAppErrorCodeInvalidFallsBackToStatusDefault(t *testing.T) {
	tests := []string{"abc", "2147483648", "1.5", ""}
	for _, value := range tests {
		trailers := metadata.Pairs("app-code", value)
		result := extractGRPCAppError(trailers, codes.InvalidArgument, "bad", testAppErrorKeys, nil, configtypes.RPCTransformError{})
		require.Equal(t, int32(400), result.AppCode, value)
	}
}

func TestExtractGRPCAppErrorCodeDefaultsPerStatus(t *testing.T) {
	tests := []struct {
		status codes.Code
		want   int32
	}{
		{codes.Unauthenticated, 401},
		{codes.PermissionDenied, 403},
		{codes.ResourceExhausted, 420},
		{codes.InvalidArgument, 400},
		{codes.NotFound, 500},
		{codes.Internal, 500},
	}
	for _, tt := range tests {
		result := extractGRPCAppError(nil, tt.status, "", testAppErrorKeys, nil, configtypes.RPCTransformError{})
		require.Equal(t, tt.want, result.AppCode, tt.status.String())
	}
}

func TestExtractGRPCAppErrorTypeFromTrailer(t *testing.T) {
	trailers := metadata.Pairs("app-type", "QUOTA_EXCEEDED_X")
	result := extractGRPCAppError(trailers, codes.ResourceExhausted, "quota", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, "QUOTA_EXCEEDED_X", result.AppType)
}

func TestExtractGRPCAppErrorTypeInvalidTrailerFallsBackToMessage(t *testing.T) {
	// A trailer with lower case or spaces does not match the pattern, so the message is used.
	trailers := metadata.Pairs("app-type", "not valid")
	result := extractGRPCAppError(trailers, codes.FailedPrecondition, "ACCOUNT_LOCKED", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, "ACCOUNT_LOCKED", result.AppType)
}

func TestExtractGRPCAppErrorTypeFromMessage(t *testing.T) {
	result := extractGRPCAppError(nil, codes.FailedPrecondition, "STATE_CHANGED_X", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, "STATE_CHANGED_X", result.AppType)
}

func TestExtractGRPCAppErrorTypeRejectsBadFormats(t *testing.T) {
	longType := strings.Repeat("A", 65)
	invalid := []string{"lower_case", "HAS SPACE", "DASH-TYPE", "", longType}
	for _, value := range invalid {
		result := extractGRPCAppError(nil, codes.Internal, value, testAppErrorKeys, nil, configtypes.RPCTransformError{})
		require.Empty(t, result.AppType, value)
	}

	// The 64 character limit is inclusive.
	exact := strings.Repeat("A", 64)
	result := extractGRPCAppError(nil, codes.Internal, exact, testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, exact, result.AppType)
}

func TestExtractGRPCAppErrorMetadataAllowList(t *testing.T) {
	trailers := metadata.Pairs(
		"trace-ref", "abc",
		"secret", "must-not-leak",
		"app-code", "3",
	)
	result := extractGRPCAppError(trailers, codes.Internal, "", testAppErrorKeys, []string{"Trace-Ref", "missing"}, configtypes.RPCTransformError{})
	require.Equal(t, map[string]string{"trace-ref": "abc"}, result.Metadata)
}

func TestExtractGRPCAppErrorMetadataNilWhenNothingAllowed(t *testing.T) {
	trailers := metadata.Pairs("secret", "value")
	result := extractGRPCAppError(trailers, codes.Internal, "", testAppErrorKeys, []string{"trace-ref"}, configtypes.RPCTransformError{})
	require.Nil(t, result.Metadata)

	result = extractGRPCAppError(trailers, codes.Internal, "", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Nil(t, result.Metadata)
}

func TestExtractGRPCAppErrorCustomTrailerKeys(t *testing.T) {
	keys := configtypes.RPCRegistryAppError{
		CodeKey: "x-error-code",
		TypeKey: "x-error-type",
	}
	trailers := metadata.Pairs("x-error-code", "42", "x-error-type", "CUSTOM_KEY")
	result := extractGRPCAppError(trailers, codes.Internal, "", keys, nil, configtypes.RPCTransformError{})
	require.Equal(t, int32(42), result.AppCode)
	require.Equal(t, "CUSTOM_KEY", result.AppType)

	// The default keys are not read when others are configured.
	result = extractGRPCAppError(metadata.Pairs("app-code", "9"), codes.Internal, "", keys, nil, configtypes.RPCTransformError{})
	require.Equal(t, int32(500), result.AppCode)
}

func TestExtractGRPCAppErrorFirstTrailerValueWins(t *testing.T) {
	trailers := metadata.Pairs("app-code", "7", "app-code", "8")
	result := extractGRPCAppError(trailers, codes.Internal, "", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, int32(7), result.AppCode)
}
