package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRPCDedupWakeupDefaultsOn(t *testing.T) {
	conf := DefaultConfig()

	require.True(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
}

func TestRPCDedupWakeupExplicitFalseFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"rpc": {"registry": {"dedup": {"redis": {"wakeup": {"enabled": false}}}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.False(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
}

func TestRPCDedupWakeupExplicitFalseFromEnv(t *testing.T) {
	t.Setenv("CENTRIFUGO_RPC_REGISTRY_DEDUP_REDIS_WAKEUP_ENABLED", "false")

	conf, _ := getConfig(t, "")

	require.False(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
}

// A config which loaded before the wakeup was on by default, with redis enabled and a waiter cap out of range, and no
// wakeup switch, must keep loading. A cap above the maximum becomes the maximum, and a cap below the minimum becomes the
// default, each with a warning.
func TestRPCDedupWakeupCapOutOfRangeWithoutSwitchLoads(t *testing.T) {
	tests := []struct {
		name       string
		maxWaiters string
		expected   int
	}{
		{name: "above the maximum", maxWaiters: "2000", expected: 1024},
		{name: "zero", maxWaiters: "0", expected: 64},
		{name: "negative", maxWaiters: "-3", expected: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "config.json")
			content := `{"rpc": {"registry": {"dedup": {"redis": {"enabled": true, "address": ["localhost:6379"], "prefix": "centrifugo", "wakeup": {"max_waiter_nodes": ` + test.maxWaiters + `}}}}}}`
			require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

			conf, meta := getConfig(t, configFile)

			require.True(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
			require.Equal(t, test.expected, conf.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
			require.NoError(t, validateRPCDedupRedis(conf, conf.RPC.Registry.Dedup.Redis))
			require.Len(t, meta.DeprecationWarnings, 1)
		})
	}
}

// A cap set by the environment overrides the file, so an out-of-range file cap is not read when the environment sets the
// cap: the environment value is kept, with no warning.
func TestRPCDedupWakeupCapFromEnvOverridesOutOfRangeFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"rpc": {"registry": {"dedup": {"redis": {"enabled": true, "address": ["localhost:6379"], "prefix": "centrifugo", "wakeup": {"max_waiter_nodes": 5000}}}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))
	t.Setenv("CENTRIFUGO_RPC_REGISTRY_DEDUP_REDIS_WAKEUP_MAX_WAITER_NODES", "100")

	conf, meta := getConfig(t, configFile)

	require.True(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
	require.Equal(t, 100, conf.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
	require.NoError(t, validateRPCDedupRedis(conf, conf.RPC.Registry.Dedup.Redis))
	require.Empty(t, meta.DeprecationWarnings)
}

// An explicit wakeup switch keeps the cap, and validation refuses it out of range.
func TestRPCDedupWakeupCapOutOfRangeWithSwitchIsRefused(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"rpc": {"registry": {"dedup": {"redis": {"enabled": true, "address": ["localhost:6379"], "prefix": "centrifugo", "wakeup": {"enabled": true, "max_waiter_nodes": 2000}}}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, meta := getConfig(t, configFile)

	require.Equal(t, 2000, conf.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
	require.Empty(t, meta.DeprecationWarnings)
	require.ErrorContains(t, validateRPCDedupRedis(conf, conf.RPC.Registry.Dedup.Redis), "must be between 1 and 1024, got 2000")
}

// Without the dedup Redis the wakeup is ignored, so an out-of-range cap is kept as it is, with no warning.
func TestRPCDedupWakeupCapOutOfRangeWithRedisDisabledIsQuiet(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"rpc": {"registry": {"dedup": {"redis": {"wakeup": {"max_waiter_nodes": 2000}}}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, meta := getConfig(t, configFile)

	require.Equal(t, 2000, conf.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
	require.Empty(t, meta.DeprecationWarnings)
}
