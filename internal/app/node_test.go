package app

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// testNodeConfig builds the node config of cfgContainer, as an App builds it for its node.
func testNodeConfig(cfgContainer *config.Container) centrifuge.Config {
	a := &App{container: cfgContainer, metricsRegistry: prometheus.NewRegistry()}
	return a.centrifugeNodeConfig()
}

// testNodeMetrics is the metrics config of a node which a test creates without an App: the node gets a registry of
// its own, as the node of an App does, so it adds nothing to the default registry of the test process.
func testNodeMetrics() centrifuge.MetricsConfig {
	return centrifuge.MetricsConfig{RegistererGatherer: prometheus.NewRegistry()}
}

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

	nodeCfg := testNodeConfig(cfgContainer)
	require.NotNil(t, nodeCfg.GetChannelMediumOptions)

	require.True(t, nodeCfg.GetChannelMediumOptions("shared:test").SharedPositionSync)
	require.False(t, nodeCfg.GetChannelMediumOptions("plain:test").SharedPositionSync)
	require.False(t, nodeCfg.GetChannelMediumOptions("unknown:test").SharedPositionSync)
	require.True(t, nodeCfg.GetChannelMediumOptions("top").SharedPositionSync)
}

func TestCentrifugeNodeConfigReplyIncludeServerTime(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	nodeCfg := testNodeConfig(cfgContainer)
	require.True(t, nodeCfg.ReplyIncludeServerTime)
}

func TestCentrifugeNodeConfigReplyIncludeServerTimeDisabledByDefault(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)

	nodeCfg := testNodeConfig(cfgContainer)
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

	nodeCfg := testNodeConfig(cfgContainer)
	// No channel options use the medium, so Core must not look it up on every publication.
	require.Nil(t, nodeCfg.GetChannelMediumOptions)
}
