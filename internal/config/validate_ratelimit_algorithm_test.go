package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// algorithmConfig returns a config with client_command publish limits of the given algorithm and buckets.
func algorithmConfig(algorithm string, buckets configtypes.RateLimitBucketList) Config {
	cfg := DefaultConfig()
	cfg.Client.RateLimit.ClientCommand.Enabled = true
	cfg.Client.RateLimit.ClientCommand.Publish = configtypes.RateLimitChannelBuckets{
		Enabled:   true,
		Algorithm: algorithm,
		Buckets:   buckets,
	}
	return cfg
}

func TestValidateAlgorithmUnknown(t *testing.T) {
	cfg := algorithmConfig("leaky", testRateLimitBuckets(time.Second, 10))
	require.EqualError(t, validateRateLimit(cfg), `client_command.publish: algorithm "leaky" is unknown, use token_bucket, sliding_window or fixed_window`)
}

func TestValidateAlgorithmKnownNames(t *testing.T) {
	for _, algorithm := range []string{"", "token_bucket", "sliding_window", "fixed_window"} {
		cfg := algorithmConfig(algorithm, testRateLimitBuckets(time.Second, 10))
		require.NoError(t, validateRateLimit(cfg), algorithm)
	}
}

func TestValidateAlgorithmClientErrorTokenOnly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Client.RateLimit.ClientError.Enabled = true
	cfg.Client.RateLimit.ClientError.Total = configtypes.RateLimitBuckets{
		Enabled:   true,
		Algorithm: "sliding_window",
		Buckets:   testRateLimitBuckets(time.Second, 5),
	}
	require.EqualError(t, validateRateLimit(cfg), `client_error.total: algorithm "sliding_window" is not supported, client_error uses token_bucket only`)

	cfg.Client.RateLimit.ClientError.Total.Algorithm = "token_bucket"
	require.NoError(t, validateRateLimit(cfg))
}

func TestValidateWindowMinInterval(t *testing.T) {
	cfg := algorithmConfig("sliding_window", testRateLimitBuckets(500*time.Millisecond, 10))
	require.EqualError(t, validateRateLimit(cfg), `client_command.publish: bucket interval must be at least 1s for algorithm sliding_window`)
}

func TestValidateWindowWholeMilliseconds(t *testing.T) {
	cfg := algorithmConfig("fixed_window", testRateLimitBuckets(time.Second+500*time.Microsecond, 10))
	require.EqualError(t, validateRateLimit(cfg), `client_command.publish: bucket interval must be a whole number of milliseconds for algorithm fixed_window`)
}

func TestValidateWindowMaxInterval(t *testing.T) {
	cfg := algorithmConfig("fixed_window", testRateLimitBuckets(time.Hour+time.Second, 10))
	require.EqualError(t, validateRateLimit(cfg), `client_command.publish: bucket interval must not be greater than 1h for algorithm fixed_window`)

	cfg = algorithmConfig("fixed_window", testRateLimitBuckets(time.Hour, 10))
	require.NoError(t, validateRateLimit(cfg))
}

func TestValidateWindowMaxRate(t *testing.T) {
	cfg := algorithmConfig("fixed_window", testRateLimitBuckets(time.Second, 2097153))
	require.EqualError(t, validateRateLimit(cfg), `client_command.publish: bucket rate must not be greater than 2097152 for algorithm fixed_window`)

	cfg = algorithmConfig("fixed_window", testRateLimitBuckets(time.Second, 2097152))
	require.NoError(t, validateRateLimit(cfg))

	cfg = algorithmConfig("sliding_window", testRateLimitBuckets(time.Hour, 1000000))
	require.NoError(t, validateRateLimit(cfg))
}

func TestValidateWindowLimitsOnlyForWindows(t *testing.T) {
	cfg := algorithmConfig("", testRateLimitBuckets(500*time.Millisecond, 10))
	require.NoError(t, validateRateLimit(cfg))
}

func TestValidateDisabledAlgorithmIgnored(t *testing.T) {
	cfg := algorithmConfig("leaky", testRateLimitBuckets(time.Second, 10))
	cfg.Client.RateLimit.ClientCommand.Publish.Enabled = false
	require.NoError(t, validateRateLimit(cfg))
}

func TestValidateAlgorithmInOverrides(t *testing.T) {
	cfg := algorithmConfig("", testRateLimitBuckets(time.Second, 10))
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat"}}
	cfg.Client.RateLimit.ClientCommand.Publish.NamespaceOverrides = configtypes.RateLimitNamespaceOverrides{
		{NamespaceName: "chat", Enabled: true, Algorithm: "leaky", Buckets: testRateLimitBuckets(time.Second, 5)},
	}
	require.ErrorContains(t, validateRateLimit(cfg), `namespace override chat: algorithm "leaky" is unknown, use token_bucket, sliding_window or fixed_window`)

	cfg = algorithmConfig("", testRateLimitBuckets(time.Second, 10))
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverrides: configtypes.RateLimitMethodOverrides{
			{Method: "status.ping", Enabled: true, Algorithm: "leaky", Buckets: testRateLimitBuckets(time.Second, 5)},
		},
	}
	require.ErrorContains(t, validateRateLimit(cfg), `method override status.ping: algorithm "leaky" is unknown`)
}

func TestValidateMethodOverrideNameWithTilde(t *testing.T) {
	cfg := algorithmConfig("", testRateLimitBuckets(time.Second, 10))
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverrides: configtypes.RateLimitMethodOverrides{
			{Method: "a~b", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 5)},
		},
	}
	require.EqualError(t, validateRateLimit(cfg), `client.rate_limit method_overrides: method name "a~b" must not contain "~"`)
}

func TestValidateMethodOverrideNameWithBrace(t *testing.T) {
	cfg := algorithmConfig("", testRateLimitBuckets(time.Second, 10))
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverrides: configtypes.RateLimitMethodOverrides{
			{Method: "a}.refresh", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 5)},
		},
	}
	require.EqualError(t, validateRateLimit(cfg), `client.rate_limit method_overrides: method name "a}.refresh" must not contain "{" or "}"`)
}
