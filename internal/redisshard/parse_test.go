package redisshard

import (
	"net"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// closedAddress returns a local address which refuses connections: the port was just released.
func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func TestParseRedisShardConfigsValid(t *testing.T) {
	configs, err := ParseRedisShardConfigs(configtypes.Redis{Address: []string{"127.0.0.1:6390"}})
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.Equal(t, "127.0.0.1:6390", configs[0].Address)
	// Defaults are applied to the returned configs.
	require.NotZero(t, configs[0].IOTimeout)
}

func TestParseRedisShardConfigsConfigurationErrors(t *testing.T) {
	_, err := ParseRedisShardConfigs(configtypes.Redis{})
	require.ErrorContains(t, err, "no Redis address configured")

	_, err = ParseRedisShardConfigs(configtypes.Redis{Address: []string{"not a valid address"}})
	require.ErrorContains(t, err, "malformed connection address")

	_, err = ParseRedisShardConfigs(configtypes.Redis{SentinelAddress: []string{"127.0.0.1:26379"}})
	require.ErrorContains(t, err, "master name must be set when using Redis Sentinel")
}

func TestConfigsOfNamesTheSettingInErrors(t *testing.T) {
	configs, err := ConfigsOf(configtypes.Redis{Address: []string{"127.0.0.1:6390"}}, "setting.redis")
	require.NoError(t, err)
	require.Len(t, configs, 1)

	_, err = ConfigsOf(configtypes.Redis{}, "setting.redis")
	require.ErrorContains(t, err, "setting.redis: ")
	require.ErrorContains(t, err, "no Redis address configured")
}

func TestNewRedisShardsFailsWhenRedisIsUnreachable(t *testing.T) {
	configs, err := ParseRedisShardConfigs(configtypes.Redis{Address: []string{closedAddress(t)}})
	require.NoError(t, err)

	shards, err := NewRedisShards(configs)
	require.Error(t, err)
	require.Nil(t, shards)
}
