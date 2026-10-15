package app

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
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

func TestCentrifugeNodeConfigChannelMediumOptionsKeepLatestPublication(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
		{
			Name: "latest",
			ChannelOptions: configtypes.ChannelOptions{
				KeepLatestPublication: true,
				AllowedDeltaTypes:     []centrifuge.DeltaType{centrifuge.DeltaTypeFossil},
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

	latestOpts := nodeCfg.GetChannelMediumOptions("latest:test")
	require.True(t, latestOpts.KeepLatestPublication)
	require.False(t, latestOpts.SharedPositionSync)
	plainOpts := nodeCfg.GetChannelMediumOptions("plain:test")
	require.False(t, plainOpts.KeepLatestPublication)
	require.False(t, plainOpts.SharedPositionSync)
}

func TestCentrifugeNodeConfigChannelMediumOptionsOnlyKeepLatestPublication(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.WithoutNamespace = configtypes.ChannelOptions{
		KeepLatestPublication: true,
		AllowedDeltaTypes:     []centrifuge.DeltaType{centrifuge.DeltaTypeFossil},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
	require.NotNil(t, nodeCfg.GetChannelMediumOptions)
	require.True(t, nodeCfg.GetChannelMediumOptions("top").KeepLatestPublication)
}

func TestCentrifugeNodeConfigReplyIncludeServerTime(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
	require.True(t, nodeCfg.ReplyIncludeServerTime)
}

func TestCentrifugeNodeConfigReplyIncludeServerTimeDisabledByDefault(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)

	nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
	require.False(t, nodeCfg.ReplyIncludeServerTime)
}

func TestCentrifugeNodeConfigChannelMediumOptionsUnsetWithoutMedium(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
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
	// No channel options use the medium, so Core must not look it up on every publication.
	require.Nil(t, nodeCfg.GetChannelMediumOptions)
}
