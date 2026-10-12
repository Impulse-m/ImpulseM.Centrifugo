package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleflightDefaults(t *testing.T) {
	conf := DefaultConfig()

	require.False(t, conf.Singleflight.Enabled)
}

func TestSingleflightFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"singleflight": {"enabled": true}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.True(t, conf.Singleflight.Enabled)
}

func TestSingleflightFromEnv(t *testing.T) {
	t.Setenv("CENTRIFUGO_SINGLEFLIGHT_ENABLED", "true")

	conf, _ := getConfig(t, "")

	require.True(t, conf.Singleflight.Enabled)
}
