package config

import (
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/stretchr/testify/require"
)

// dedupConfig returns a valid config with dedup enabled, for the dedup validation tests.
func dedupConfig() Config {
	cfg := configWithRegistry(validInlineRegistry())
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry.Dedup = configtypes.RPCDedup{
		Enabled:                  true,
		Window:                   configtypes.Duration(120 * time.Second),
		MaxSkew:                  configtypes.Duration(30 * time.Second),
		MaxReplySize:             65536,
		LocalMaxEntries:          200000,
		LocalMaxBytes:            268435456,
		LocalAnonymousMaxEntries: 20000,
		LocalAnonymousMaxBytes:   33554432,
		ScopeReserve:             4,
		ScopeReserveBytes:        8192,
		MaxScopesPerUser:         8,
		MaxRecordsPerScope:       1024,
		OnStoreError:             configtypes.RPCDedupFailClosed,
	}
	return cfg
}

// TestValidateRPCDedupCancelledErrorBoundary checks that the cancelled error is accepted up to the reply limit of its
// encoded form, and refused one byte over it. The limit is the one both tiers keep, see configtypes.RPCDedup.ReplyLimit.
func TestValidateRPCDedupCancelledErrorBoundary(t *testing.T) {
	cfg := dedupConfig()
	limit := cfg.RPC.Registry.Dedup.ReplyLimit()
	cancelled := cfg.RPC.Registry.Errors.Cancelled
	encodedSize := func(n int) int {
		return rpcdedup.ErrorSize(&centrifuge.Error{
			Code:    cancelled.Code,
			Message: strings.Repeat("e", n),
			AppCode: cancelled.AppCode,
			AppType: cancelled.AppType,
		})
	}
	n := 0
	for encodedSize(n+1) <= limit {
		n++
	}
	cfg.RPC.Registry.Errors.Cancelled.Message = strings.Repeat("e", n)
	require.NoError(t, cfg.Validate())

	cfg.RPC.Registry.Errors.Cancelled.Message = strings.Repeat("e", n+1)
	require.ErrorContains(t, cfg.Validate(), "rpc.registry.errors.cancelled.message: the encoded cancelled error must fit the reply limit")
}

func TestValidateRPCDedupDisabledIsNotChecked(t *testing.T) {
	cfg := configWithRegistry(validInlineRegistry())
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
			name: "local max bytes must be positive",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalMaxBytes = 0
			},
			wantErr: "rpc.registry.dedup.local_max_bytes: must be positive",
		},
		{
			name: "max records per scope must be positive",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.MaxRecordsPerScope = 0
			},
			wantErr: "rpc.registry.dedup.max_records_per_scope: must be positive",
		},
		{
			name: "scope reserve must be at least one record",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.ScopeReserve = 0
			},
			wantErr: "rpc.registry.dedup.scope_reserve: must be at least 1",
		},
		{
			name: "scope reserve bytes must be at least 1024",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.ScopeReserveBytes = 1023
			},
			wantErr: "rpc.registry.dedup.scope_reserve_bytes: must be at least 1024",
		},
		{
			name: "scope reserve bytes must hold a record per reserve",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.ScopeReserve = 100
				cfg.RPC.Registry.Dedup.MaxRecordsPerScope = 1024
			},
			wantErr: "rpc.registry.dedup.scope_reserve_bytes: must be at least scope_reserve 100 times 256 bytes per record",
		},
		{
			name: "max scopes per user must be at least one",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.MaxScopesPerUser = 0
			},
			wantErr: "rpc.registry.dedup.max_scopes_per_user: must be at least 1",
		},
		{
			name: "max records per scope must hold the reserve",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.ScopeReserve = 8
				cfg.RPC.Registry.Dedup.MaxRecordsPerScope = 4
			},
			wantErr: "rpc.registry.dedup.max_records_per_scope: must be at least scope_reserve 8",
		},
		{
			name: "cancelled error must fit the reply limit",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Errors.Cancelled.Message = strings.Repeat("e", cfg.RPC.Registry.Dedup.ReplyLimit())
			},
			wantErr: "rpc.registry.errors.cancelled.message: the encoded cancelled error must fit the reply limit",
		},
		{
			name: "anonymous entries must be below the total",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = 200000
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_entries: must be at least 0 and less than",
		},
		{
			name: "anonymous entries must not be negative",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = -1
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_entries: must be at least 0",
		},
		{
			name: "anonymous bytes must be below the total",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes = 268435456
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_bytes: must be at least 0 and less than",
		},
		{
			name: "anonymous bytes must not be negative",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes = -1
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_bytes: must be at least 0",
		},
		{
			name: "authenticated entries must hold a scope per shard",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalMaxEntries = 20000 + 64*4 - 1
			},
			wantErr: "rpc.registry.dedup.local_max_entries: local_max_entries minus local_anonymous_max_entries must be at least 256",
		},
		{
			name: "authenticated bytes must hold a scope per shard",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalMaxBytes = 33554432 + 64*8192 - 1
			},
			wantErr: "rpc.registry.dedup.local_max_bytes: local_max_bytes minus local_anonymous_max_bytes must be at least",
		},
		{
			name: "anonymous entries must hold a scope per shard",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = 64*4 - 1
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_entries: must be 0, or at least 256",
		},
		{
			name: "anonymous bytes must hold a scope per shard",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes = 64*8192 - 1
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_bytes: must be at least 524288",
		},
		{
			name: "anonymous bytes must be zero without anonymous entries",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = 0
			},
			wantErr: "rpc.registry.dedup.local_anonymous_max_bytes: must be 0 when local_anonymous_max_entries is 0",
		},
		{
			name: "anonymous connections may be left without a scope",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = 0
				cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes = 0
			},
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
			cfg := dedupConfig()
			tc.modify(&cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateRPCDedupRedis(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name: "valid",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
			},
		},
		{
			name: "reuse without a redis engine",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.ReuseFromEngine = true
				cfg.Engine.Type = "memory"
			},
			wantErr: "reuse_from_engine requires engine.type to be redis",
		},
		{
			name: "reuse with a redis engine",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.ReuseFromEngine = true
				cfg.Engine.Type = "redis"
			},
		},
		{
			name: "empty address",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
			},
			wantErr: "rpc.registry.dedup.redis:",
		},
		{
			name: "empty prefix",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = ""
			},
			wantErr: "rpc.registry.dedup.redis.prefix: must not be empty",
		},
		{
			name: "wakeup max waiter nodes of zero",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
				cfg.RPC.Registry.Dedup.Redis.Wakeup.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes = 0
			},
			wantErr: "rpc.registry.dedup.redis.wakeup.max_waiter_nodes: must be between 1 and 1024, got 0",
		},
		{
			name: "wakeup max waiter nodes above 1024",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
				cfg.RPC.Registry.Dedup.Redis.Wakeup.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes = 1025
			},
			wantErr: "rpc.registry.dedup.redis.wakeup.max_waiter_nodes: must be between 1 and 1024, got 1025",
		},
		{
			name: "wakeup max waiter nodes of one",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
				cfg.RPC.Registry.Dedup.Redis.Wakeup.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes = 1
			},
		},
		{
			name: "wakeup default max waiter nodes",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
				cfg.RPC.Registry.Dedup.Redis.Wakeup = DefaultConfig().RPC.Registry.Dedup.Redis.Wakeup
				cfg.RPC.Registry.Dedup.Redis.Wakeup.Enabled = true
			},
		},
		{
			name: "wakeup max waiter nodes out of range while the wakeup is off",
			modify: func(cfg *Config) {
				cfg.RPC.Registry.Dedup.Redis.Enabled = true
				cfg.RPC.Registry.Dedup.Redis.Address = []string{"localhost:6379"}
				cfg.RPC.Registry.Dedup.Redis.Prefix = "centrifugo"
				cfg.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes = 0
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dedupConfig()
			tc.modify(&cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestDefaultsOfRPCDedupAreOff(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.RPC.Registry.Dedup.Enabled)
	require.False(t, cfg.RPC.Registry.Dedup.Redis.Enabled)
	require.False(t, cfg.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
	require.Equal(t, 64, cfg.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
	require.Equal(t, 120*time.Second, cfg.RPC.Registry.Dedup.Window.ToDuration())
	require.Equal(t, 30*time.Second, cfg.RPC.Registry.Dedup.MaxSkew.ToDuration())
	require.Equal(t, 2048, cfg.RPC.Registry.Dedup.MaxReplySize)
	require.Equal(t, 200000, cfg.RPC.Registry.Dedup.LocalMaxEntries)
	require.Equal(t, 268435456, cfg.RPC.Registry.Dedup.LocalMaxBytes)
	require.Equal(t, 20000, cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries)
	require.Equal(t, 33554432, cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes)
	require.Equal(t, 4, cfg.RPC.Registry.Dedup.ScopeReserve)
	require.Equal(t, 8192, cfg.RPC.Registry.Dedup.ScopeReserveBytes)
	require.Equal(t, 8, cfg.RPC.Registry.Dedup.MaxScopesPerUser)
	require.Equal(t, 1024, cfg.RPC.Registry.Dedup.MaxRecordsPerScope)
	require.Equal(t, configtypes.RPCDedupFailClosed, cfg.RPC.Registry.Dedup.OnStoreError)
	require.Equal(t, uint32(107), cfg.RPC.Registry.Errors.MsgIDInvalid.Code)
	require.Equal(t, uint32(110), cfg.RPC.Registry.Errors.ResultEvicted.Code)
	require.Equal(t, uint32(107), cfg.RPC.Registry.Errors.MsgIDReused.Code)
	require.Equal(t, uint32(108), cfg.RPC.Registry.Errors.DedupUnavailable.Code)
}
