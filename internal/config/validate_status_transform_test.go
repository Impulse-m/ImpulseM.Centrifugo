package config

import (
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidateRPCRegistryGRPCStatusNames(t *testing.T) {
	validNames := []string{"UNAUTHENTICATED", "DATA_LOSS", "CANCELLED", "RESOURCE_EXHAUSTED"}
	for _, name := range validNames {
		transforms := configtypes.RPCGRPCStatusToCodeTransforms{
			{Status: name, ToError: configtypes.TransformError{Code: 101}},
		}
		require.NoError(t, validateRPCRegistryTransforms(transforms), name)
	}

	// Names are matched exactly: lower case, bare numbers and OK are rejected.
	invalidNames := []string{"unauthenticated", "Unauthenticated", "16", "1", "OK", "NOT_A_STATUS", ""}
	for _, name := range invalidNames {
		transforms := configtypes.RPCGRPCStatusToCodeTransforms{
			{Status: name, ToError: configtypes.TransformError{Code: 101}},
		}
		require.Error(t, validateRPCRegistryTransforms(transforms), name)
	}
}

func TestValidateRPCRegistryTransformTextLimits(t *testing.T) {
	nonASCII := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "UNAUTHENTICATED", ToError: configtypes.TransformError{Code: 101, Message: "café"}},
	}
	require.ErrorContains(t, validateRPCRegistryTransforms(nonASCII), "grpc_status_to_code_transforms[0] error message must be ASCII")

	tooLong := configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "UNAUTHENTICATED", ToDisconnect: configtypes.TransformDisconnect{Code: 3000, Reason: strings.Repeat("x", 124)}},
	}
	require.ErrorContains(t, validateRPCRegistryTransforms(tooLong), "grpc_status_to_code_transforms[0] disconnect reason can be up to 123 characters long")
}

func TestValidateStatusToCodeTargetSharedWithHTTP(t *testing.T) {
	// The HTTP status transform keeps its own key in messages, the same checks apply.
	err := validateStatusToCodeTarget(statusToCodeTransformsKey, 2, configtypes.TransformError{Code: 1, Message: "café"}, configtypes.TransformDisconnect{})
	require.ErrorContains(t, err, "status_to_code_transforms[2] error message must be ASCII")

	err = validateStatusTransforms([]configtypes.HttpStatusToCodeTransform{
		{StatusCode: 400, ToError: configtypes.TransformError{Code: 1, Message: strings.Repeat("x", 124)}},
	})
	require.ErrorContains(t, err, "status_to_code_transforms[0] item error message can be up to 123 characters long")

	err = validateStatusTransforms([]configtypes.HttpStatusToCodeTransform{
		{StatusCode: 400},
	})
	require.ErrorContains(t, err, "no error or disconnect code set in status_to_code_transforms[0]")

	err = validateStatusTransforms([]configtypes.HttpStatusToCodeTransform{
		{StatusCode: 400, ToError: configtypes.TransformError{Code: 1}, ToDisconnect: configtypes.TransformDisconnect{Code: 3000}},
	})
	require.ErrorContains(t, err, "only error or disconnect code can be set in status_to_code_transforms[0], but not both")

	err = validateStatusTransforms([]configtypes.HttpStatusToCodeTransform{
		{StatusCode: 400, ToDisconnect: configtypes.TransformDisconnect{Code: 3000, Reason: "café"}},
	})
	require.ErrorContains(t, err, "status_to_code_transforms[0] disconnect reason must be ASCII")
}
