package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRateLimitedCode(t *testing.T) {
	for _, code := range []uint32{0, 400, 1999} {
		reg := validInlineRegistry()
		reg.Errors.RateLimited.Code = code
		require.NoError(t, validateRPCRegistry(configWithRegistry(reg)), code)
	}
	for _, code := range []uint32{1, 113, 399, 2000} {
		reg := validInlineRegistry()
		reg.Errors.RateLimited.Code = code
		require.EqualError(t, validateRPCRegistry(configWithRegistry(reg)),
			fmt.Sprintf("rpc.registry.errors.rate_limited.code: must be 0 or between 400 and 1999, got %d", code))
	}
}

// A disabled registry is not checked, and its rate limited code is not used, so the code is not checked either.
func TestValidateRateLimitedCodeDisabledRegistry(t *testing.T) {
	reg := validInlineRegistry()
	reg.Enabled = false
	reg.Errors.RateLimited.Code = 113
	require.NoError(t, validateRPCRegistry(configWithRegistry(reg)))
}

// Error metadata names are not refused at start: the runtime drops the names with the reserved prefix.
func TestValidateErrorMetadataUnchanged(t *testing.T) {
	for _, names := range [][]string{
		{"x-centrifugo-retry-after"},
		{"authorization"},
		{"kept-key"},
	} {
		reg := validInlineRegistry()
		reg.Methods[0].ErrorMetadata = names
		require.NoError(t, validateRPCRegistry(configWithRegistry(reg)), names)
	}

	reg := validInlineRegistry()
	reg.Methods[0].ErrorMetadata = []string{"X-Foo"}
	require.ErrorContains(t, validateRPCRegistry(configWithRegistry(reg)), "must be lower-case")
}
