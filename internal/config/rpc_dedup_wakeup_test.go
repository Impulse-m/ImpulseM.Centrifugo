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
// wakeup switch, must keep loading. The cap is replaced by the default with a warning.
func TestRPCDedupWakeupCapOutOfRangeWithoutSwitchLoads(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"rpc": {"registry": {"dedup": {"redis": {"enabled": true, "address": ["localhost:6379"], "prefix": "centrifugo", "wakeup": {"max_waiter_nodes": 2000}}}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, meta := getConfig(t, configFile)

	require.True(t, conf.RPC.Registry.Dedup.Redis.Wakeup.Enabled)
	require.Equal(t, 64, conf.RPC.Registry.Dedup.Redis.Wakeup.MaxWaiterNodes)
	require.NoError(t, validateRPCDedupRedis(conf, conf.RPC.Registry.Dedup.Redis))
	require.Len(t, meta.DeprecationWarnings, 1)
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
