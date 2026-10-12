package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestAccessRevokeDefaults(t *testing.T) {
	conf := DefaultConfig()

	require.True(t, conf.UserBlock.Enabled)
	require.Equal(t, "memory", conf.UserBlock.StorageType)
	require.True(t, conf.TokenRevoke.Enabled)
	require.Equal(t, "memory", conf.TokenRevoke.StorageType)
	require.Equal(t, "jti", conf.TokenRevoke.UIDClaim)
	require.True(t, conf.UserTokensInvalidate.Enabled)
	require.Equal(t, "memory", conf.UserTokensInvalidate.StorageType)
	require.Empty(t, conf.UserTokensInvalidate.GenerationClaim)

	require.Equal(t, 1000, conf.AccessRevoke.DisconnectRate)
	require.Equal(t, 100000, conf.AccessRevoke.DisconnectQueueSize)
	require.Equal(t, 100, conf.AccessRevoke.NotifyRate)
	require.Equal(t, configtypes.Duration(20*time.Minute), conf.AccessRevoke.DefaultTTL)
	require.Equal(t, configtypes.Duration(30*time.Second), conf.AccessRevoke.SweepInterval)
}

func TestAccessRevokeFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"user_block": {"enabled": false},
		"token_revoke": {"uid_claim": "sid"},
		"user_tokens_invalidate": {"generation_claim": "gen"},
		"access_revoke": {"disconnect_rate": 50, "default_ttl": "5m", "sweep_interval": "10s"}
	}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	// Explicit false must not be turned back into true by defaults.
	require.False(t, conf.UserBlock.Enabled)
	// Keys which are not set keep their defaults.
	require.True(t, conf.TokenRevoke.Enabled)
	require.True(t, conf.UserTokensInvalidate.Enabled)
	require.Equal(t, "sid", conf.TokenRevoke.UIDClaim)
	require.Equal(t, "gen", conf.UserTokensInvalidate.GenerationClaim)
	require.Equal(t, 50, conf.AccessRevoke.DisconnectRate)
	require.Equal(t, 100000, conf.AccessRevoke.DisconnectQueueSize)
	require.Equal(t, configtypes.Duration(5*time.Minute), conf.AccessRevoke.DefaultTTL)
	require.Equal(t, configtypes.Duration(10*time.Second), conf.AccessRevoke.SweepInterval)
}

func TestValidateAccessRevoke(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name:   "defaults are valid",
			modify: func(_ *Config) {},
		},
		{
			name: "memory storage type is valid",
			modify: func(c *Config) {
				c.UserBlock.StorageType = "memory"
				c.TokenRevoke.StorageType = "memory"
				c.UserTokensInvalidate.StorageType = "memory"
			},
		},
		{
			name: "empty storage type is valid",
			modify: func(c *Config) {
				c.UserBlock.StorageType = ""
			},
		},
		{
			name: "unknown user block storage type is invalid",
			modify: func(c *Config) {
				c.UserBlock.StorageType = "redis"
			},
			wantErr: "unknown user_block.storage_type",
		},
		{
			name: "unknown token revoke storage type is invalid",
			modify: func(c *Config) {
				c.TokenRevoke.StorageType = "redis"
			},
			wantErr: "unknown token_revoke.storage_type",
		},
		{
			name: "unknown user tokens invalidate storage type is invalid",
			modify: func(c *Config) {
				c.UserTokensInvalidate.StorageType = "redis"
			},
			wantErr: "unknown user_tokens_invalidate.storage_type",
		},
		{
			name: "invalid uid claim is invalid",
			modify: func(c *Config) {
				c.TokenRevoke.UIDClaim = "session-id"
			},
			wantErr: "invalid token_revoke.uid_claim",
		},
		{
			name: "invalid generation claim is invalid",
			modify: func(c *Config) {
				c.UserTokensInvalidate.GenerationClaim = "gen-1"
			},
			wantErr: "invalid user_tokens_invalidate.generation_claim",
		},
		{
			name: "zero disconnect rate is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.DisconnectRate = 0
			},
			wantErr: "access_revoke.disconnect_rate must be greater than zero",
		},
		{
			name: "zero disconnect queue size is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.DisconnectQueueSize = 0
			},
			wantErr: "access_revoke.disconnect_queue_size must be greater than zero",
		},
		{
			name: "zero notify rate is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.NotifyRate = 0
			},
			wantErr: "access_revoke.notify_rate must be greater than zero",
		},
		{
			name: "zero default ttl is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.DefaultTTL = 0
			},
			wantErr: "access_revoke.default_ttl must be greater than zero",
		},
		{
			name: "zero sweep interval is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.SweepInterval = 0
			},
			wantErr: "access_revoke.sweep_interval must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAccessRevokeSourceDefaults(t *testing.T) {
	conf := DefaultConfig()

	require.Empty(t, conf.AccessRevoke.Source.Type)
	require.Equal(t, configtypes.Duration(30*time.Minute), conf.AccessRevoke.Source.Window)
	require.Equal(t, configtypes.Duration(60*time.Second), conf.AccessRevoke.Source.StartupTimeout)
	require.Equal(t, configtypes.Duration(30*time.Second), conf.AccessRevoke.Source.LagThreshold)
	require.False(t, conf.AccessRevoke.Source.RejectConnectOnLag)
	require.Equal(t, "centrifugo-method", conf.AccessRevoke.Source.Kafka.MethodHeader)
	require.Equal(t, configtypes.Duration(2*time.Second), conf.AccessRevoke.Source.Kafka.FetchMaxWait)
}

func TestAccessRevokeSourceFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"access_revoke": {
			"source": {
				"type": "kafka",
				"window": "40m",
				"startup_timeout": "90s",
				"lag_threshold": "10s",
				"reject_connect_on_lag": true,
				"kafka": {
					"brokers": ["localhost:9092"],
					"topic": "access_revoke",
					"fetch_max_wait": "1s",
					"sasl_mechanism": "plain",
					"sasl_user": "user",
					"sasl_password": "secret"
				}
			}
		}
	}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	source := conf.AccessRevoke.Source
	require.Equal(t, "kafka", source.Type)
	require.Equal(t, configtypes.Duration(40*time.Minute), source.Window)
	require.Equal(t, configtypes.Duration(90*time.Second), source.StartupTimeout)
	require.Equal(t, configtypes.Duration(10*time.Second), source.LagThreshold)
	require.True(t, source.RejectConnectOnLag)
	require.Equal(t, []string{"localhost:9092"}, source.Kafka.Brokers)
	require.Equal(t, "access_revoke", source.Kafka.Topic)
	require.Equal(t, configtypes.Duration(time.Second), source.Kafka.FetchMaxWait)
	require.Equal(t, "centrifugo-method", source.Kafka.MethodHeader)
	require.Equal(t, "plain", source.Kafka.SASLMechanism)
	require.Equal(t, "user", source.Kafka.SASLUser)
	require.Equal(t, "secret", source.Kafka.SASLPassword)
}

func TestValidateAccessRevokeSource(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name:   "no source is valid",
			modify: func(_ *Config) {},
		},
		{
			name: "empty source type ignores other source fields",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Window = 0
				c.AccessRevoke.Source.Kafka.Brokers = nil
			},
		},
		{
			name: "unknown source type is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "nats"
			},
			wantErr: "unknown access_revoke.source.type",
		},
		{
			name: "valid kafka source",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.Kafka.Brokers = []string{"localhost:9092"}
				c.AccessRevoke.Source.Kafka.Topic = "access_revoke"
			},
		},
		{
			name: "kafka source without brokers is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.Kafka.Topic = "access_revoke"
			},
			wantErr: "access_revoke.source.kafka.brokers must be set",
		},
		{
			name: "kafka source without topic is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.Kafka.Brokers = []string{"localhost:9092"}
			},
			wantErr: "access_revoke.source.kafka.topic must be set",
		},
		{
			name: "window shorter than default ttl is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.Kafka.Brokers = []string{"localhost:9092"}
				c.AccessRevoke.Source.Kafka.Topic = "access_revoke"
				c.AccessRevoke.Source.Window = configtypes.Duration(time.Minute)
			},
			wantErr: "access_revoke.source.window must be greater or equal to access_revoke.default_ttl",
		},
		{
			name: "zero window is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.Window = 0
			},
			wantErr: "access_revoke.source.window must be greater than zero",
		},
		{
			name: "zero startup timeout is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.StartupTimeout = 0
			},
			wantErr: "access_revoke.source.startup_timeout must be greater than zero",
		},
		{
			name: "zero lag threshold is invalid",
			modify: func(c *Config) {
				c.AccessRevoke.Source.Type = "kafka"
				c.AccessRevoke.Source.LagThreshold = 0
			},
			wantErr: "access_revoke.source.lag_threshold must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
