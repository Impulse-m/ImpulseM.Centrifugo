package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestMigrationCopiesAlgorithm(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Client.RateLimit.ClientCommand.Enabled = true
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverride: configtypes.RateLimitMethodOverrideMap{
			"status.ping": {Enabled: true, Algorithm: "fixed_window", Buckets: testRateLimitBuckets(time.Minute, 5)},
		},
	}

	migrated, _ := applyConfigMigrations(cfg)
	overrides := migrated.Client.RateLimit.ClientCommand.RPC.MethodOverrides
	require.Len(t, overrides, 1)
	require.Equal(t, "fixed_window", overrides[0].Algorithm)
	require.Equal(t, "status.ping", overrides[0].Method)
}
