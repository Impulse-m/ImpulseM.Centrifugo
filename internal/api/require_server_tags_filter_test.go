package api

import (
	"context"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// requireTestExecutor returns a server API executor whose namespace "req" requires a server tags filter, and whose
// channels without namespace do not.
func requireTestExecutor(t *testing.T) *Executor {
	t.Helper()
	node := nodeWithMemoryEngine()
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "req",
			ChannelOptions: configtypes.ChannelOptions{
				RequireServerTagsFilter: true,
			},
		},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	return NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
}

// TestSubscribeAPIRefusesRequiredServerTagsFilter checks a server API subscription is refused on a channel of a namespace
// which requires a server tags filter, since the request cannot carry one, and allowed on a channel without the option.
func TestSubscribeAPIRefusesRequiredServerTagsFilter(t *testing.T) {
	api := requireTestExecutor(t)
	resp := api.Subscribe(context.Background(), &SubscribeRequest{
		User:    "test",
		Channel: "req:room",
	})
	require.Equal(t, ErrorNotAvailable, resp.Error)

	resp = api.Subscribe(context.Background(), &SubscribeRequest{
		User:    "test",
		Channel: "test",
	})
	require.Nil(t, resp.Error)
}

// TestBatchSubscribeAPIRefusesRequiredServerTagsFilter checks the refusal is the same through a batch subscribe command.
func TestBatchSubscribeAPIRefusesRequiredServerTagsFilter(t *testing.T) {
	api := requireTestExecutor(t)
	resp := api.Batch(context.Background(), &BatchRequest{
		Commands: []*Command{
			{Subscribe: &SubscribeRequest{User: "test", Channel: "req:room"}},
		},
	})
	require.Len(t, resp.Replies, 1)
	require.Equal(t, ErrorNotAvailable, resp.Replies[0].Error)
}
