package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestApplyNamespaceSubscribeOptionsBandwidth(t *testing.T) {
	chOpts := configtypes.ChannelOptions{
		AllowChannelCompaction:        true,
		ClientPublishDebounceInterval: configtypes.Duration(50 * time.Millisecond),
	}

	options := centrifuge.SubscribeOptions{}
	applyNamespaceSubscribeOptions(&options, chOpts, "room", centrifuge.SubscriptionTypeStream)
	require.True(t, options.AllowChannelCompaction)
	require.Equal(t, 50*time.Millisecond, options.ClientPublishDebounceInterval)

	emptyOptions := centrifuge.SubscribeOptions{}
	applyNamespaceSubscribeOptions(&emptyOptions, configtypes.ChannelOptions{}, "room", centrifuge.SubscriptionTypeStream)
	require.False(t, emptyOptions.AllowChannelCompaction)
	require.Zero(t, emptyOptions.ClientPublishDebounceInterval)
}
