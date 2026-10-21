package config

import (
	"fmt"
	"maps"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// migrateRateLimitMethodOverride converts deprecated method_override map into method_overrides array.
// When both are set nothing is migrated, validation reports the conflict.
func migrateRateLimitMethodOverride(rpc *configtypes.RateLimitRPCBuckets, path string, warnings []string) []string {
	if len(rpc.MethodOverride) == 0 || len(rpc.MethodOverrides) > 0 {
		return warnings
	}
	methods := slices.Sorted(maps.Keys(rpc.MethodOverride))
	overrides := make(configtypes.RateLimitMethodOverrides, 0, len(methods))
	for _, method := range methods {
		item := rpc.MethodOverride[method]
		overrides = append(overrides, configtypes.RateLimitMethodOverride{
			Method:    method,
			Enabled:   item.Enabled,
			Algorithm: item.Algorithm,
			Buckets:   item.Buckets,
		})
	}
	rpc.MethodOverrides = overrides
	rpc.MethodOverride = nil
	return append(warnings, fmt.Sprintf("%s.method_override is deprecated and will be removed in Centrifugo v7, use %s.method_overrides instead", path, path))
}
