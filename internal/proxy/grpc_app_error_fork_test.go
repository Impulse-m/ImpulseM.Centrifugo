package proxy

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var testBackendUnavailableFallback = configtypes.RPCTransformError{
	AppCode: 503,
	AppType: "BACKEND_UNAVAILABLE",
}

func TestExtractGRPCAppErrorRuleFillsMissingFields(t *testing.T) {
	result := extractGRPCAppError(nil, codes.Unavailable, "connection refused", testAppErrorKeys, nil, testBackendUnavailableFallback)
	require.Equal(t, int32(503), result.AppCode)
	require.Equal(t, "BACKEND_UNAVAILABLE", result.AppType)
}

func TestExtractGRPCAppErrorTrailersBeatRule(t *testing.T) {
	trailers := metadata.Pairs("app-code", "429", "app-type", "QUOTA_EXCEEDED_17")
	result := extractGRPCAppError(trailers, codes.Unavailable, "connection refused", testAppErrorKeys, nil, testBackendUnavailableFallback)
	require.Equal(t, int32(429), result.AppCode)
	require.Equal(t, "QUOTA_EXCEEDED_17", result.AppType)
}

func TestExtractGRPCAppErrorStatusMessageTypeBeatsRule(t *testing.T) {
	result := extractGRPCAppError(nil, codes.Unavailable, "QUOTA_EXCEEDED", testAppErrorKeys, nil, testBackendUnavailableFallback)
	require.Equal(t, "QUOTA_EXCEEDED", result.AppType)
	require.Equal(t, int32(503), result.AppCode)
}

func TestExtractGRPCAppErrorFieldsFallBackSeparately(t *testing.T) {
	// Only the code trailer: the code comes from the trailer, the type from the rule.
	result := extractGRPCAppError(metadata.Pairs("app-code", "7"), codes.Unavailable, "connection refused", testAppErrorKeys, nil, testBackendUnavailableFallback)
	require.Equal(t, int32(7), result.AppCode)
	require.Equal(t, "BACKEND_UNAVAILABLE", result.AppType)

	// Both trailers invalid: both fields come from the rule.
	trailers := metadata.Pairs("app-code", "abc", "app-type", "bad type")
	result = extractGRPCAppError(trailers, codes.Unavailable, "connection refused", testAppErrorKeys, nil, testBackendUnavailableFallback)
	require.Equal(t, int32(503), result.AppCode)
	require.Equal(t, "BACKEND_UNAVAILABLE", result.AppType)
}

func TestExtractGRPCAppErrorZeroRuleKeepsDefaults(t *testing.T) {
	result := extractGRPCAppError(nil, codes.Unauthenticated, "", testAppErrorKeys, nil, configtypes.RPCTransformError{})
	require.Equal(t, int32(401), result.AppCode)
	require.Empty(t, result.AppType)
}
