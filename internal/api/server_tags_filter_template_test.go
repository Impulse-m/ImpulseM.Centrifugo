package api

import (
	"context"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// TestSubscribeAPIRefusesServerTagsFilterTemplate checks a server API subscription is refused on a channel whose
// namespace has a server tags filter template, since no connection values exist to render it with.
func TestSubscribeAPIRefusesServerTagsFilterTemplate(t *testing.T) {
	node := nodeWithMemoryEngine()
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "tenant",
			ChannelOptions: configtypes.ChannelOptions{
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "tenant", "cmp": "eq", "val": "{{claims.tenant}}"},
				},
			},
		},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	api := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
	resp := api.Subscribe(context.Background(), &SubscribeRequest{
		User:    "test",
		Channel: "tenant:room",
	})
	require.Equal(t, ErrorNotAvailable, resp.Error)
}
