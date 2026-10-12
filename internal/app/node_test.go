package app

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestCentrifugeNodeConfigChannelMediumOptions(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.WithoutNamespace = configtypes.ChannelOptions{
		ForcePositioning:   true,
		SharedPositionSync: true,
	}
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
		{
			Name: "shared",
			ChannelOptions: configtypes.ChannelOptions{
				ForcePositioning:   true,
				SharedPositionSync: true,
			},
		},
		{
			Name: "plain",
			ChannelOptions: configtypes.ChannelOptions{
				ForcePositioning: true,
			},
		},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
	require.NotNil(t, nodeCfg.GetChannelMediumOptions)

	require.True(t, nodeCfg.GetChannelMediumOptions("shared:test").SharedPositionSync)
	require.False(t, nodeCfg.GetChannelMediumOptions("plain:test").SharedPositionSync)
	require.False(t, nodeCfg.GetChannelMediumOptions("unknown:test").SharedPositionSync)
	require.True(t, nodeCfg.GetChannelMediumOptions("top").SharedPositionSync)
}
