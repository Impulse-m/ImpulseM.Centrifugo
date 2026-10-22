package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestGetConfigRateLimitFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
  "channel": {"namespaces": [{"name": "chat"}]},
  "client": {
    "rate_limit": {
      "client_command": {
        "enabled": true,
        "default": {"enabled": true, "buckets": [{"interval": "1s", "rate": 10}]},
        "publish": {
          "enabled": true,
          "buckets": [{"interval": "1s", "rate": 5}],
          "namespace_overrides": [{"namespace_name": "chat", "enabled": true, "buckets": [{"interval": "1s", "rate": 20}]}]
        },
        "rpc": {
          "enabled": true,
          "buckets": [{"interval": "1s", "rate": 10}],
          "method_override": {"update_user_status": {"enabled": true, "buckets": [{"interval": "20s", "rate": 1}]}}
        }
      },
      "client_error": {"enabled": true, "total": {"enabled": true, "buckets": [{"interval": "5s", "rate": 20}]}}
    }
  }
}`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	conf, meta, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.NoError(t, conf.Validate())
	require.Len(t, meta.DeprecationWarnings, 1)

	cc := conf.Client.RateLimit.ClientCommand
	require.True(t, cc.Enabled)
	require.Equal(t, 10, cc.Default.Buckets[0].Rate)
	require.Equal(t, time.Second, cc.Publish.Buckets[0].Interval.ToDuration())
	require.Len(t, cc.Publish.NamespaceOverrides, 1)
	require.Equal(t, "chat", cc.Publish.NamespaceOverrides[0].NamespaceName)
	require.Len(t, cc.RPC.MethodOverrides, 1)
	require.Equal(t, "update_user_status", cc.RPC.MethodOverrides[0].Method)
	require.Nil(t, cc.RPC.MethodOverride)
	require.Equal(t, 20, conf.Client.RateLimit.ClientError.Total.Buckets[0].Rate)
}

func TestGetConfigRateLimitAllSectionsFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
  "channel": {"namespaces": [{"name": "chat"}]},
  "engine": {"type": "redis"},
  "client": {
    "rate_limit": {
      "client_command": {
        "enabled": true,
        "total": {"enabled": true, "buckets": [{"interval": "1s", "rate": 30}]},
        "subscribe": {"enabled": true, "buckets": [{"interval": "1m", "rate": 4}]},
        "refresh": {"enabled": true, "buckets": [{"interval": "1m", "rate": 6}]}
      },
      "user_command": {
        "enabled": true,
        "default": {"enabled": true, "buckets": [{"interval": "1s", "rate": 11}]},
        "connect": {"enabled": true, "buckets": [{"interval": "1m", "rate": 3}]},
        "publish": {
          "enabled": true,
          "buckets": [{"interval": "1s", "rate": 7}],
          "namespace_overrides": [{"namespace_name": "chat", "enabled": true, "buckets": [{"interval": "1s", "rate": 17}]}]
        },
        "rpc": {"enabled": true, "buckets": [{"interval": "1s", "rate": 9}]}
      },
      "redis_user_command": {
        "enabled": true,
        "redis": {"reuse_from_engine": false, "prefix": "rl", "address": ["redis://127.0.0.1:6380"]},
        "default": {"enabled": true, "buckets": [{"interval": "1s", "rate": 13}]},
        "connect": {"enabled": true, "buckets": [{"interval": "1m", "rate": 2}]},
        "history": {"enabled": true, "buckets": [{"interval": "1s", "rate": 5}]},
        "sub_refresh": {"enabled": true, "buckets": [{"interval": "1s", "rate": 8}]}
      },
      "client_error": {"enabled": true, "total": {"enabled": true, "buckets": [{"interval": "5s", "rate": 21}]}}
    }
  }
}`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.NoError(t, conf.Validate())
	rl := conf.Client.RateLimit

	cc := rl.ClientCommand
	require.True(t, cc.Enabled)
	require.Equal(t, 30, cc.Total.Buckets[0].Rate)
	require.Equal(t, 4, cc.Subscribe.Buckets[0].Rate)
	require.Equal(t, time.Minute, cc.Subscribe.Buckets[0].Interval.ToDuration())
	require.Equal(t, 6, cc.Refresh.Buckets[0].Rate)
	require.Empty(t, cc.Publish.Buckets)

	uc := rl.UserCommand
	require.True(t, uc.Enabled)
	require.Equal(t, 11, uc.Default.Buckets[0].Rate)
	require.Equal(t, 3, uc.Connect.Buckets[0].Rate)
	require.Equal(t, 7, uc.Publish.Buckets[0].Rate)
	require.Equal(t, 17, uc.Publish.NamespaceOverrides[0].Buckets[0].Rate)
	require.Equal(t, 9, uc.RPC.Buckets[0].Rate)
	require.Empty(t, uc.Total.Buckets)

	rc := rl.RedisUserCommand
	require.True(t, rc.Enabled)
	require.Equal(t, "rl", rc.Redis.RedisPrefixed.Prefix)
	require.Equal(t, []string{"redis://127.0.0.1:6380"}, rc.Redis.RedisPrefixed.Redis.Address)
	require.Equal(t, 13, rc.Default.Buckets[0].Rate)
	require.Equal(t, 2, rc.Connect.Buckets[0].Rate)
	require.Equal(t, 5, rc.History.Buckets[0].Rate)
	require.Equal(t, 8, rc.SubRefresh.Buckets[0].Rate)
	require.Empty(t, rc.Publish.Buckets)

	require.Equal(t, 21, rl.ClientError.Total.Buckets[0].Rate)
}

func testRateLimitBuckets(interval time.Duration, rate int) configtypes.RateLimitBucketList {
	return configtypes.RateLimitBucketList{{Interval: configtypes.Duration(interval), Rate: rate}}
}

func testRateLimitContainer(interval time.Duration, rate int) configtypes.RateLimitBuckets {
	return configtypes.RateLimitBuckets{Enabled: true, Buckets: testRateLimitBuckets(interval, rate)}
}

func TestValidateRateLimit(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *Config)
		wantErr string
	}{
		{
			name:   "disabled by default",
			mutate: func(cfg *Config) {},
		},
		{
			name: "client command with default and total",
			mutate: func(cfg *Config) {
				cc := &cfg.Client.RateLimit.ClientCommand
				cc.Enabled = true
				cc.Total = testRateLimitContainer(time.Second, 20)
				cc.Default = testRateLimitContainer(time.Second, 60)
			},
		},
		{
			name: "namespace override with no buckets is allowed",
			mutate: func(cfg *Config) {
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat"}}
				cc := &cfg.Client.RateLimit.ClientCommand
				cc.Enabled = true
				cc.Default = testRateLimitContainer(time.Second, 10)
				cc.Publish = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "chat", Enabled: true},
					},
				}
			},
		},
		{
			name: "rpc method overrides",
			mutate: func(cfg *Config) {
				cc := &cfg.Client.RateLimit.ClientCommand
				cc.Enabled = true
				cc.RPC = configtypes.RateLimitRPCBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 10),
					MethodOverrides: configtypes.RateLimitMethodOverrides{
						{Method: "update_user_status", Enabled: true, Buckets: testRateLimitBuckets(20*time.Second, 1)},
					},
				}
			},
		},
		{
			name: "client error total",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientError.Enabled = true
				cfg.Client.RateLimit.ClientError.Total = testRateLimitContainer(5*time.Second, 20)
			},
		},
		{
			name: "enabled default without buckets",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Default.Enabled = true
			},
			wantErr: "client_command.default: enabled buckets must not be empty",
		},
		{
			name: "enabled total without buckets",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Total.Enabled = true
			},
			wantErr: "client_command.total: enabled buckets must not be empty",
		},
		{
			name: "enabled base operation without buckets",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Publish.Enabled = true
			},
			wantErr: "client_command.publish: enabled buckets must not be empty",
		},
		{
			name: "enabled client error total without buckets",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientError.Enabled = true
				cfg.Client.RateLimit.ClientError.Total.Enabled = true
			},
			wantErr: "client_error.total: enabled buckets must not be empty",
		},
		{
			name: "enabled client error without total",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientError.Enabled = true
			},
			wantErr: "client_error: enabled without total bucket has no effect",
		},
		{
			name: "zero interval",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Default = testRateLimitContainer(0, 10)
			},
			wantErr: "bucket interval must be greater than 0",
		},
		{
			name: "zero rate",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Default = testRateLimitContainer(time.Second, 0)
			},
			wantErr: "bucket rate must be greater than 0",
		},
		{
			name: "interval too small for rate",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Default = testRateLimitContainer(time.Nanosecond, 2)
			},
			wantErr: "too small for rate 2",
		},
		{
			name: "duplicate namespace override",
			mutate: func(cfg *Config) {
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat"}}
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Publish = configtypes.RateLimitChannelBuckets{
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "chat", Enabled: true},
						{NamespaceName: "chat", Enabled: true},
					},
				}
			},
			wantErr: "duplicate namespace_name in namespace_overrides: chat",
		},
		{
			name: "unknown namespace override",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.Publish = configtypes.RateLimitChannelBuckets{
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "missing", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
					},
				}
			},
			wantErr: "namespace_name missing in namespace_overrides not found",
		},
		{
			name: "method_override and method_overrides together",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
					MethodOverride: configtypes.RateLimitMethodOverrideMap{
						"a": {Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
					},
					MethodOverrides: configtypes.RateLimitMethodOverrides{
						{Method: "b", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
					},
				}
			},
			wantErr: "method_override and method_overrides cannot be used together",
		},
		{
			name: "duplicate method override",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
					MethodOverrides: configtypes.RateLimitMethodOverrides{
						{Method: "a", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
						{Method: "a", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 2)},
					},
				}
			},
			wantErr: "duplicate method in method_overrides: a",
		},
		{
			name: "map and shared poll operations with namespace override",
			mutate: func(cfg *Config) {
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat"}}
				cfg.Client.RateLimit.ClientCommand.Enabled = true
				cfg.Client.RateLimit.ClientCommand.MapPublish = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "chat", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
					},
				}
				cfg.Client.RateLimit.ClientCommand.MapRemove = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
				}
				cfg.Client.RateLimit.ClientCommand.Track = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
				}
				cfg.Client.RateLimit.ClientCommand.Untrack = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
				}
			},
		},
		{
			name: "map_publish unknown namespace override",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.UserCommand.Enabled = true
				cfg.Client.RateLimit.UserCommand.MapPublish = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 5),
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "missing", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
					},
				}
			},
			wantErr: "user_command.map_publish: namespace_name missing in namespace_overrides not found",
		},
		{
			name: "track invalid bucket rate",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.RedisUserCommand.Enabled = true
				cfg.Client.RateLimit.RedisUserCommand.Track = configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: testRateLimitBuckets(time.Second, 0),
				}
			},
			wantErr: "redis_user_command.track: bucket rate must be greater than 0",
		},
		{
			name: "redis user command total",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.RedisUserCommand.Total = testRateLimitContainer(time.Second, 10)
			},
			wantErr: "redis_user_command.total is not supported",
		},
		{
			name: "reuse from engine without redis engine",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.RedisUserCommand.Enabled = true
				cfg.Client.RateLimit.RedisUserCommand.Redis.ReuseFromEngine = true
			},
			wantErr: "reuse_from_engine requires engine.type to be redis",
		},
		{
			name: "user command is implemented",
			mutate: func(cfg *Config) {
				cfg.Client.RateLimit.UserCommand.Enabled = true
			},
		},
		{
			name: "redis user command is implemented",
			mutate: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Client.RateLimit.RedisUserCommand.Enabled = true
				cfg.Client.RateLimit.RedisUserCommand.Redis.ReuseFromEngine = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestMigrateRateLimitMethodOverride(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Client.RateLimit.ClientCommand.Enabled = true
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverride: configtypes.RateLimitMethodOverrideMap{
			"update_user_status": {Enabled: true, Buckets: testRateLimitBuckets(20*time.Second, 1)},
			"get_user_data":      {Enabled: true, Buckets: testRateLimitBuckets(5*time.Second, 5)},
		},
	}

	migrated, warnings := applyConfigMigrations(cfg)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "client.rate_limit.client_command.rpc.method_override is deprecated")

	rpc := migrated.Client.RateLimit.ClientCommand.RPC
	require.Nil(t, rpc.MethodOverride)
	require.Len(t, rpc.MethodOverrides, 2)
	require.Equal(t, "get_user_data", rpc.MethodOverrides[0].Method)
	require.Equal(t, "update_user_status", rpc.MethodOverrides[1].Method)
	require.Equal(t, testRateLimitBuckets(20*time.Second, 1), rpc.MethodOverrides[1].Buckets)
	require.NoError(t, migrated.Validate())
}

func TestMigrateRateLimitMethodOverrideBothSetNotMigrated(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Client.RateLimit.ClientCommand.Enabled = true
	cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 10),
		MethodOverride: configtypes.RateLimitMethodOverrideMap{
			"a": {Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
		},
		MethodOverrides: configtypes.RateLimitMethodOverrides{
			{Method: "b", Enabled: true, Buckets: testRateLimitBuckets(time.Second, 1)},
		},
	}

	migrated, warnings := applyConfigMigrations(cfg)
	require.Empty(t, warnings)
	require.Len(t, migrated.Client.RateLimit.ClientCommand.RPC.MethodOverride, 1)
	require.ErrorContains(t, migrated.Validate(), "cannot be used together")
}

func TestValidateRedisUserCommandOnError(t *testing.T) {
	withOnError := func(onError string) Config {
		cfg := DefaultConfig()
		cfg.Client.RateLimit.RedisUserCommand.Enabled = true
		cfg.Client.RateLimit.RedisUserCommand.OnError = onError
		return cfg
	}

	require.NoError(t, withOnError("").Validate())
	require.NoError(t, withOnError(configtypes.RateLimitOnErrorAllow).Validate())
	require.NoError(t, withOnError(configtypes.RateLimitOnErrorDeny).Validate())
	require.ErrorContains(t, withOnError("block").Validate(), "redis_user_command.on_error must be allow or deny")
}

func TestValidateRedisUserCommandRejectsNegativeCommandTimeout(t *testing.T) {
	withTimeout := func(timeout time.Duration) Config {
		cfg := DefaultConfig()
		cfg.Client.RateLimit.RedisUserCommand.Enabled = true
		cfg.Client.RateLimit.RedisUserCommand.CommandTimeout = configtypes.Duration(timeout)
		return cfg
	}

	require.NoError(t, withTimeout(0).Validate())
	require.NoError(t, withTimeout(20*time.Millisecond).Validate())
	require.ErrorContains(t, withTimeout(-20*time.Millisecond).Validate(), "redis_user_command.command_timeout must not be negative")
}

func TestValidateRedisBucketRejectsSubMicrosecondEmission(t *testing.T) {
	withRate := func(interval time.Duration, rate int) Config {
		cfg := DefaultConfig()
		cfg.Client.RateLimit.RedisUserCommand.Enabled = true
		cfg.Client.RateLimit.RedisUserCommand.Publish = configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testRateLimitBuckets(interval, rate),
		}
		return cfg
	}

	// Emission of 1s / 1000000 is exactly 1us, which Redis keeps. 1s / 2000000 is 500ns, which Redis would raise to 1us.
	require.NoError(t, withRate(time.Second, 1000000).Validate())
	require.ErrorContains(t, withRate(time.Second, 2000000).Validate(), "redis_user_command.publish: bucket emission interval must be at least 1us")

	// Only the Redis token bucket truncates, so the same bucket is valid for user_command.
	cfg := DefaultConfig()
	cfg.Client.RateLimit.UserCommand.Enabled = true
	cfg.Client.RateLimit.UserCommand.Publish = configtypes.RateLimitChannelBuckets{
		Enabled: true,
		Buckets: testRateLimitBuckets(time.Second, 2000000),
	}
	require.NoError(t, cfg.Validate())
}

func testRateLimitBucketsOf(count int) configtypes.RateLimitBucketList {
	buckets := make(configtypes.RateLimitBucketList, 0, count)
	for i := 0; i < count; i++ {
		buckets = append(buckets, configtypes.RateLimitBucket{Interval: configtypes.Duration(time.Second), Rate: 10})
	}
	return buckets
}

func TestValidateRateLimitMethodOverridesCount(t *testing.T) {
	overridesOf := func(count int) configtypes.RateLimitMethodOverrides {
		overrides := make(configtypes.RateLimitMethodOverrides, 0, count)
		for i := 0; i < count; i++ {
			overrides = append(overrides, configtypes.RateLimitMethodOverride{
				Method:  fmt.Sprintf("method_%d", i),
				Enabled: true,
				Buckets: testRateLimitBuckets(time.Second, 1),
			})
		}
		return overrides
	}
	withOverrides := func(count int) Config {
		cfg := DefaultConfig()
		cfg.Client.RateLimit.ClientCommand.Enabled = true
		cfg.Client.RateLimit.ClientCommand.RPC = configtypes.RateLimitRPCBuckets{
			Enabled:         true,
			Buckets:         testRateLimitBuckets(time.Second, 10),
			MethodOverrides: overridesOf(count),
		}
		return cfg
	}

	require.NoError(t, withOverrides(rateLimitMaxMethodOverrides).Validate())
	require.ErrorContains(t, withOverrides(rateLimitMaxMethodOverrides+1).Validate(), "client_command.rpc: method_overrides must not have more than 4096 entries")
}

func TestValidateRateLimitBucketsCount(t *testing.T) {
	withBuckets := func(count int) Config {
		cfg := DefaultConfig()
		cfg.Client.RateLimit.ClientCommand.Enabled = true
		cfg.Client.RateLimit.ClientCommand.Publish = configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testRateLimitBucketsOf(count),
		}
		return cfg
	}

	require.NoError(t, withBuckets(rateLimitMaxBuckets).Validate())
	require.ErrorContains(t, withBuckets(rateLimitMaxBuckets+1).Validate(), "client_command.publish: buckets must not have more than 16 entries")
}
