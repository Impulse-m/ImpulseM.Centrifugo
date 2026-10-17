package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestConfigBandwidthOptionsJSON(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{
  "channel": {
    "without_namespace": {
      "allow_channel_compaction": true,
      "client_publish_debounce_interval": "50ms"
    },
    "namespaces": [
      {
        "name": "bandwidth",
        "allowed_delta_types": ["fossil"],
        "keep_latest_publication": true,
        "allow_channel_compaction": true,
        "client_publish_debounce_interval": "1s"
      }
    ]
  }
}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0o600))

	conf, meta := getConfig(t, configFile)
	require.Len(t, meta.UnknownKeys, 0)
	require.True(t, conf.Channel.WithoutNamespace.AllowChannelCompaction)
	require.Equal(t, configtypes.Duration(50*time.Millisecond), conf.Channel.WithoutNamespace.ClientPublishDebounceInterval)
	require.Len(t, conf.Channel.Namespaces, 1)
	namespace := conf.Channel.Namespaces[0].ChannelOptions
	require.True(t, namespace.KeepLatestPublication)
	require.True(t, namespace.AllowChannelCompaction)
	require.Equal(t, configtypes.Duration(time.Second), namespace.ClientPublishDebounceInterval)
}

func TestConfigBandwidthOptionsEnvVars(t *testing.T) {
	t.Setenv("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_CLIENT_PUBLISH_DEBOUNCE_INTERVAL", "50ms")
	conf, meta := getConfig(t, "testdata/config.json")
	require.Equal(t, configtypes.Duration(50*time.Millisecond), conf.Channel.WithoutNamespace.ClientPublishDebounceInterval)
	require.Len(t, meta.UnknownEnvs, 0)
}
