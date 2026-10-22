package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// rateLimitedAppTypeOf returns an enabled registry whose rate limited error has the app type appType.
func rateLimitedAppTypeOf(appType string) configtypes.RPCRegistry {
	reg := validInlineRegistry()
	reg.Errors.RateLimited.Code = 417
	reg.Errors.RateLimited.AppType = appType
	return reg
}

// The placeholder is valid at the end of the app type, after an underscore, so the expanded type ends in _<digits>
// and the errors counter normalises every wait to _X.
func TestValidateRateLimitedAppTypePlaceholder(t *testing.T) {
	for _, appType := range []string{
		"QUOTA_WAIT_{retry_after}",
		"QUOTA_{retry_after}",
		strings.Repeat("A", 53) + "_{retry_after}",
	} {
		require.NoError(t, validateRPCRegistry(configWithRegistry(rateLimitedAppTypeOf(appType))), appType)
	}
}

// The placeholder must occur once, at the end after an underscore. Any other place gives a series for each wait.
func TestValidateRateLimitedAppTypePlaceholderPosition(t *testing.T) {
	for _, appType := range []string{
		"{retry_after}",
		"QUOTA{retry_after}",
		"QUOTA_{retry_after}_WAIT",
		"QUOTA_{retry_after}_WAIT_{retry_after}",
		"QUOTA_{retry_after}_{retry_after}",
		"QUOTA_WAIT_{retry_after}_",
	} {
		require.EqualError(t, validateRPCRegistry(configWithRegistry(rateLimitedAppTypeOf(appType))),
			fmt.Sprintf("rpc.registry.errors.rate_limited.app_type: {retry_after} must occur once, at the end after an underscore, as in QUOTA_WAIT_{retry_after}, got %q", appType), appType)
	}
}

// A placeholder is checked with its widest value, ten digits, so every wait the limits can produce fits the pattern.
func TestValidateRateLimitedAppTypePlaceholderInvalid(t *testing.T) {
	for _, appType := range []string{
		"QUOTA_WAIT_{retry_after",
		"quota_{retry_after}",
		"QUOTA WAIT_{retry_after}",
		"QUOTA_WAIT_{retry-after}",
		strings.Repeat("A", 54) + "_{retry_after}",
	} {
		require.EqualError(t, validateRPCRegistry(configWithRegistry(rateLimitedAppTypeOf(appType))),
			fmt.Sprintf("rpc.registry.errors.rate_limited.app_type: must match ^[A-Z0-9_]{1,64}$ with {retry_after} replaced by 9999999999, got %q", appType), appType)
	}
}

// A disabled registry is not checked, as with its other settings, so the app type of rate_limited is read only when
// the registry is on.
func TestValidateRateLimitedAppTypeDisabledRegistryUnchecked(t *testing.T) {
	reg := rateLimitedAppTypeOf("QUOTA_{retry_after}_WAIT_{retry_after}")
	reg.Enabled = false
	require.NoError(t, validateRPCRegistry(configWithRegistry(reg)))
}

// Plain app types of rate_limited are not checked against the pattern, as before the placeholder.
func TestValidateRateLimitedAppTypePlainUnchanged(t *testing.T) {
	require.NoError(t, validateRPCRegistry(configWithRegistry(rateLimitedAppTypeOf("example.throttle"))))
}

// The placeholder is allowed only in rate_limited.app_type. Every other protocol error refuses it.
func TestValidateRetryAfterPlaceholderOnlyInRateLimited(t *testing.T) {
	fields := []struct {
		name  string
		field func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError
	}{
		{"unknown_method", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.UnknownMethod }},
		{"auth_class", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.AuthClass }},
		{"request_too_large", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.RequestTooLarge }},
		{"response_too_large", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.ResponseTooLarge }},
		{"cancelled", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.Cancelled }},
		{"msg_id_invalid", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.MsgIDInvalid }},
		{"msg_id_reused", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.MsgIDReused }},
		{"result_evicted", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.ResultEvicted }},
		{"overloaded", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.Overloaded }},
		{"token_expired", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.TokenExpired }},
		{"dedup_unavailable", func(errs *configtypes.RPCRegistryErrors) *configtypes.RPCRegistryError { return &errs.DedupUnavailable }},
	}
	for _, f := range fields {
		reg := validInlineRegistry()
		f.field(&reg.Errors).AppType = "QUOTA_WAIT_{retry_after}"
		require.EqualError(t, validateRPCRegistry(configWithRegistry(reg)),
			fmt.Sprintf("rpc.registry.errors.%s.app_type: {retry_after} is allowed only in rpc.registry.errors.rate_limited.app_type", f.name), f.name)
	}
}

// A transform fills an app type of the protocol error, which takes no placeholder, so the pattern refuses it.
func TestValidateRetryAfterPlaceholderRefusedInTransform(t *testing.T) {
	reg := validInlineRegistry()
	reg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
		{
			Status: "UNAVAILABLE",
			ToError: configtypes.RPCTransformError{
				TransformError: configtypes.TransformError{Code: 100},
				AppType:        "QUOTA_WAIT_{retry_after}",
			},
		},
	}
	require.ErrorContains(t, validateRPCRegistry(configWithRegistry(reg)), "to_error.app_type must match ^[A-Z0-9_]{1,64}$")
}
