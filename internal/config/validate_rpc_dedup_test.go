package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// dedupConfig returns a valid config with dedup enabled, for the dedup validation tests.
func dedupConfig() Config {
	var cfg Config = configWithRegistry(validInlineRegistry())
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry.Dedup = configtypes.RPCDedup{
		Enabled:         true,
		Window:          configtypes.Duration(120 * time.Second),
		MaxSkew:         configtypes.Duration(30 * time.Second),
		MaxReplySize:    65536,
		LocalMaxEntries: 200000,
		OnStoreError:    configtypes.RPCDedupFailClosed,
	}
	return cfg
}

func TestValidateRPCDedupDisabledIsNotChecked(t *testing.T) {
	var cfg Config = configWithRegistry(validInlineRegistry())
	cfg.RPC.Registry.Dedup.Window = configtypes.Duration(time.Second)
	cfg.RPC.Registry.Dedup.MaxSkew = configtypes.Duration(time.Hour)
	require.NoError(t, cfg.Validate())
}

func TestValidateRPCDedupRules(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name:   "valid",
			modify: func(cfg *Config) {},
		},
		{
			name: "needs the registry",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Enabled = false
			},
			wantErr: "requires rpc.registry.enabled",
		},
		{
			name: "needs the server time in replies",
			modify: func(cfg *Config) {
				cfg.Client.ReplyIncludeServerTime = false
			},
			wantErr: "requires client.reply_include_server_time",
		},
		{
			name: "window must be at least a second",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Window = configtypes.Duration(500 * time.Millisecond)
				cfg.RPC.Registry.Dedup.MaxSkew = 0
			},
			wantErr: "rpc.registry.dedup.window: must be at least 1s",
		},
		{
			name: "max skew must not be negative",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.MaxSkew = configtypes.Duration(-time.Second)
			},
			wantErr: "rpc.registry.dedup.max_skew: must not be negative",
		},
		{
			name: "window must be greater than max skew",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.MaxSkew = configtypes.Duration(120 * time.Second)
			},
			wantErr: "must be greater than max_skew",
		},
		{
			name: "max reply size must be positive",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.MaxReplySize = 0
			},
			wantErr: "rpc.registry.dedup.max_reply_size: must be positive",
		},
		{
			name: "local max entries must be positive",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalMaxEntries = 0
			},
			wantErr: "rpc.registry.dedup.local_max_entries: must be positive",
		},
		{
			name: "unknown store error mode",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.OnStoreError = "retry"
			},
			wantErr: "rpc.registry.dedup.on_store_error: must be one of",
		},
		{
			name: "fail open is valid",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config = dedupConfig()
			tc.modify(&cfg)
			var err error = cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestDefaultsOfRPCDedupAreOff(t *testing.T) {
	var cfg Config = DefaultConfig()
	require.False(t, cfg.RPC.Registry.Dedup.Enabled)
	require.Equal(t, 120*time.Second, cfg.RPC.Registry.Dedup.Window.ToDuration())
	require.Equal(t, 30*time.Second, cfg.RPC.Registry.Dedup.MaxSkew.ToDuration())
	require.Equal(t, 65536, cfg.RPC.Registry.Dedup.MaxReplySize)
	require.Equal(t, 200000, cfg.RPC.Registry.Dedup.LocalMaxEntries)
	require.Equal(t, configtypes.RPCDedupFailClosed, cfg.RPC.Registry.Dedup.OnStoreError)
	require.Equal(t, uint32(107), cfg.RPC.Registry.Errors.MsgIDInvalid.Code)
	require.Equal(t, uint32(110), cfg.RPC.Registry.Errors.ResultEvicted.Code)
	require.Equal(t, uint32(107), cfg.RPC.Registry.Errors.MsgIDReused.Code)
	require.Equal(t, uint32(108), cfg.RPC.Registry.Errors.DedupUnavailable.Code)
}
