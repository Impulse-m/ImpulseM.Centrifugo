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
