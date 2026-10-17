package app

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

// TestCentrifugeNodeConfigClientChannelLimit checks that client.channel_limit reaches the node, which applies it to every
// connection, pending ones included. The client package tests the limit on a node they build themselves, so this test
// protects the wiring of the config.
func TestCentrifugeNodeConfigClientChannelLimit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.ChannelLimit = 7
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
	require.Equal(t, 7, nodeCfg.ClientChannelLimit)
}
