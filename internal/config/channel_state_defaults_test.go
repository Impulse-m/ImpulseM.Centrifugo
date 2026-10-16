package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestChannelStateDefaults(t *testing.T) {
	conf := DefaultConfig()

	require.Equal(t, configtypes.Duration(5*time.Second), conf.Engine.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 128, conf.Engine.Redis.ChannelState.NumPartitions)
	require.Equal(t, configtypes.Duration(5*time.Second), conf.PresenceManager.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 128, conf.PresenceManager.Redis.ChannelState.NumPartitions)
}

func TestChannelStateFromFileAbsentKeysGetDefaults(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"engine": {"redis": {"channel_state": {"num_partitions": 8}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.Equal(t, 8, conf.Engine.Redis.ChannelState.NumPartitions)
	require.Equal(t, configtypes.Duration(5*time.Second), conf.Engine.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 128, conf.PresenceManager.Redis.ChannelState.NumPartitions)
	require.Equal(t, configtypes.Duration(5*time.Second), conf.PresenceManager.Redis.ChannelState.VacatedEventDelay)
}

func TestChannelStateFromFileValueIsKept(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"presence_manager": {"redis": {"channel_state": {"vacated_event_delay": "250ms", "num_partitions": 16}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.Equal(t, configtypes.Duration(250*time.Millisecond), conf.PresenceManager.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 16, conf.PresenceManager.Redis.ChannelState.NumPartitions)
	require.Equal(t, configtypes.Duration(5*time.Second), conf.Engine.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 128, conf.Engine.Redis.ChannelState.NumPartitions)
}

// TestChannelStateFromFileZeroIsKept checks that an explicit zero in the config file keeps its meaning:
// no grace period for vacated_event_delay, and a validation error for num_partitions.
func TestChannelStateFromFileZeroIsKept(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"engine": {"redis": {"channel_state": {"vacated_event_delay": "0s", "num_partitions": 0}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.Equal(t, configtypes.Duration(0), conf.Engine.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 0, conf.Engine.Redis.ChannelState.NumPartitions)
	require.ErrorContains(t, conf.Validate(), "in engine.redis.channel_state: num_partitions must be between 1 and 4096, got 0")
}

func TestChannelStateFromFileZeroDelayInPresenceManagerIsKept(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.json")
	content := `{"presence_manager": {"redis": {"channel_state": {"vacated_event_delay": "0s"}}}}`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0600))

	conf, _ := getConfig(t, configFile)

	require.Equal(t, configtypes.Duration(0), conf.PresenceManager.Redis.ChannelState.VacatedEventDelay)
	require.Equal(t, 128, conf.PresenceManager.Redis.ChannelState.NumPartitions)
	require.NoError(t, conf.Validate())
}
