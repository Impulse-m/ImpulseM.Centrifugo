package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// validChannelStateConfig returns a config with state and cache empty events enabled on the
// without_namespace options, backed by the Redis engine. Every test case changes one thing from it.
func validChannelStateConfig() Config {
	cfg := DefaultConfig()
	cfg.Engine.Type = "redis"
	cfg.Channel.Proxy.State = configtypes.Proxy{
		Endpoint: "http://localhost:8000/channel_state",
		Timeout:  configtypes.Duration(time.Second),
	}
	cfg.Channel.Proxy.CacheEmpty = configtypes.Proxy{
		Endpoint: "http://localhost:8000/cache_empty",
		Timeout:  configtypes.Duration(time.Second),
	}
	opts := &cfg.Channel.WithoutNamespace
	opts.Presence = true
	opts.StateProxyEnabled = true
	opts.CacheEmptyProxyEnabled = true
	opts.CacheEmptyProxyName = "default"
	opts.ForceRecoveryMode = "cache"
	return cfg
}

func TestValidateChannelState(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *Config)
		wantErr string
	}{
		{
			name:   "valid with engine redis",
			mutate: func(cfg *Config) {},
		},
		{
			name: "valid with presence manager redis and engine memory",
			mutate: func(cfg *Config) {
				cfg.Engine.Type = "memory"
				cfg.PresenceManager.Enabled = true
				cfg.PresenceManager.Type = "redis"
			},
		},
		{
			name: "state proxy requires presence",
			mutate: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.Presence = false
			},
			wantErr: "state_proxy_enabled requires presence",
		},
		{
			name: "state proxy without endpoint",
			mutate: func(cfg *Config) {
				cfg.Channel.Proxy.State.Endpoint = ""
			},
			wantErr: "in channel.proxy.state: endpoint not set",
		},
		{
			name: "state proxy without timeout",
			mutate: func(cfg *Config) {
				cfg.Channel.Proxy.State.Timeout = 0
			},
			wantErr: "in channel.proxy.state: timeout not set",
		},
		{
			name: "state proxy requires redis presence manager",
			mutate: func(cfg *Config) {
				cfg.Engine.Type = "memory"
			},
			wantErr: "state_proxy_enabled requires the redis presence manager",
		},
		{
			name: "state proxy with enabled memory presence manager and redis engine",
			mutate: func(cfg *Config) {
				cfg.PresenceManager.Enabled = true
				cfg.PresenceManager.Type = "memory"
			},
			wantErr: "state_proxy_enabled requires the redis presence manager",
		},
		{
			name: "zero num partitions in engine",
			mutate: func(cfg *Config) {
				cfg.Engine.Redis.ChannelState.NumPartitions = 0
			},
			wantErr: "in engine.redis.channel_state: num_partitions must be between 1 and 4096",
		},
		{
			name: "too many num partitions in presence manager",
			mutate: func(cfg *Config) {
				cfg.PresenceManager.Redis.ChannelState.NumPartitions = 4097
			},
			wantErr: "in presence_manager.redis.channel_state: num_partitions must be between 1 and 4096",
		},
		{
			name: "negative vacated event delay in engine",
			mutate: func(cfg *Config) {
				cfg.Engine.Redis.ChannelState.VacatedEventDelay = configtypes.Duration(-time.Second)
			},
			wantErr: "in engine.redis.channel_state: vacated_event_delay must not be negative",
		},
		{
			name: "negative vacated event delay in presence manager",
			mutate: func(cfg *Config) {
				cfg.PresenceManager.Redis.ChannelState.VacatedEventDelay = configtypes.Duration(-time.Second)
			},
			wantErr: "in presence_manager.redis.channel_state: vacated_event_delay must not be negative",
		},
		{
			name: "zero vacated event delay is valid",
			mutate: func(cfg *Config) {
				cfg.Engine.Redis.ChannelState.VacatedEventDelay = 0
			},
		},
		{
			name: "cache empty proxy name not found",
			mutate: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.CacheEmptyProxyName = "unknown"
			},
			wantErr: "cache empty proxy with name \"unknown\" not found",
		},
		{
			name: "cache empty proxy with empty name",
			mutate: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.CacheEmptyProxyName = ""
			},
			wantErr: "cache empty proxy with name \"\" not found",
		},
		{
			name: "cache empty proxy name not checked when disabled",
			mutate: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.CacheEmptyProxyEnabled = false
				cfg.Channel.WithoutNamespace.CacheEmptyProxyName = "unknown"
			},
		},
		{
			name: "cache empty default proxy without endpoint",
			mutate: func(cfg *Config) {
				cfg.Channel.Proxy.CacheEmpty.Endpoint = ""
			},
			wantErr: "in channel.proxy.cache_empty: endpoint not set",
		},
		{
			name: "cache empty requires cache recovery mode",
			mutate: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.ForceRecoveryMode = "stream"
			},
			wantErr: "cache_empty_proxy_enabled requires force_recovery_mode \"cache\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validChannelStateConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
