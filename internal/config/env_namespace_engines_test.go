package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// checkNamespaceEnginesConfig checks the named entries of testdata/namespace_engines.* as each format decodes them.
func checkNamespaceEnginesConfig(t *testing.T, conf Config) {
	t.Helper()
	require.Len(t, conf.Brokers, 1)
	require.Equal(t, "sample", conf.Brokers[0].Name)
	require.Equal(t, "redis", conf.Brokers[0].Type)
	require.Equal(t, []string{"redis://10.0.0.5:6379"}, conf.Brokers[0].Redis.Address)
	require.Equal(t, "sample", conf.Brokers[0].Redis.Prefix)
	require.Len(t, conf.PresenceManagers, 1)
	require.Equal(t, "sample", conf.PresenceManagers[0].Name)
	require.Equal(t, "memory", conf.PresenceManagers[0].Type)
	require.Len(t, conf.MapBrokers, 1)
	require.Equal(t, "sample", conf.MapBrokers[0].Name)
	require.True(t, conf.MapBrokers[0].Enabled)
	require.Equal(t, "redis", conf.MapBrokers[0].Type)
	require.Equal(t, []string{"redis://10.0.0.6:6379"}, conf.MapBrokers[0].Redis.Address)
	require.Equal(t, "sample", conf.MapBrokers[0].Redis.Prefix)
}

func TestNamespaceEnginesJSON(t *testing.T) {
	conf, _ := getConfig(t, "testdata/namespace_engines.json")
	checkNamespaceEnginesConfig(t, conf)
}

func TestNamespaceEnginesYAML(t *testing.T) {
	conf, _ := getConfig(t, "testdata/namespace_engines.yaml")
	checkNamespaceEnginesConfig(t, conf)
}

func TestNamespaceEnginesTOML(t *testing.T) {
	conf, _ := getConfig(t, "testdata/namespace_engines.toml")
	checkNamespaceEnginesConfig(t, conf)
}

func TestConfigEnvVarsNamedEngines(t *testing.T) {
	_ = os.Setenv("CENTRIFUGO_BROKERS", `fast slow`)
	_ = os.Setenv("CENTRIFUGO_BROKERS_FAST_TYPE", `redis`)
	_ = os.Setenv("CENTRIFUGO_BROKERS_FAST_REDIS_ADDRESS", `redis://10.0.0.2:6379`)
	_ = os.Setenv("CENTRIFUGO_PRESENCE_MANAGERS", `[{"name":"p","enabled":true,"type":"redis"}]`)
	defer func() {
		_ = os.Unsetenv("CENTRIFUGO_BROKERS")
		_ = os.Unsetenv("CENTRIFUGO_BROKERS_FAST_TYPE")
		_ = os.Unsetenv("CENTRIFUGO_BROKERS_FAST_REDIS_ADDRESS")
		_ = os.Unsetenv("CENTRIFUGO_PRESENCE_MANAGERS")
	}()
	conf, meta := getConfig(t, "testdata/namespace_engines.json")
	require.Len(t, meta.UnknownEnvs, 0)
	// Space-separated names are appended to the brokers loaded from the file, as for consumers.
	require.Len(t, conf.Brokers, 3)
	require.Equal(t, "sample", conf.Brokers[0].Name)
	require.Equal(t, "fast", conf.Brokers[1].Name)
	require.Equal(t, "redis", conf.Brokers[1].Type)
	require.Equal(t, []string{"redis://10.0.0.2:6379"}, conf.Brokers[1].Redis.Address)
	require.Equal(t, "slow", conf.Brokers[2].Name)
	require.Equal(t, "memory", conf.Brokers[2].Type)
	require.Equal(t, "centrifugo", conf.Brokers[2].Redis.Prefix)
	require.Len(t, conf.PresenceManagers, 1)
	require.Equal(t, "p", conf.PresenceManagers[0].Name)
	require.Equal(t, "redis", conf.PresenceManagers[0].Type)
}

func TestConfigEnvVarsNamedMapBrokers(t *testing.T) {
	_ = os.Setenv("CENTRIFUGO_MAP_BROKERS_SAMPLE_REDIS_PREFIX", `from-env`)
	defer func() {
		_ = os.Unsetenv("CENTRIFUGO_MAP_BROKERS_SAMPLE_REDIS_PREFIX")
	}()
	conf, meta := getConfig(t, "testdata/namespace_engines.json")
	require.Len(t, meta.UnknownEnvs, 0)
	// The env variables set the fields of the entry of the file with the same name.
	require.Len(t, conf.MapBrokers, 1)
	require.Equal(t, "sample", conf.MapBrokers[0].Name)
	require.Equal(t, "redis", conf.MapBrokers[0].Type)
	require.Equal(t, "from-env", conf.MapBrokers[0].Redis.Prefix)
}
