package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestClientOnSubscribeNamespaceBandwidthOptions(t *testing.T) {
	testCases := []struct {
		name               string
		subscribeForClient bool
		subscribeProxy     bool
		useToken           bool
	}{
		{
			name:               "namespace permission",
			subscribeForClient: true,
		},
		{
			name:     "subscription token",
			useToken: true,
		},
		{
			name:           "subscribe proxy",
			subscribeProxy: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := tools.NodeWithMemoryEngineNoHandlers()
			defer func() { _ = node.Shutdown(context.Background()) }()

			cfg := config.DefaultConfig()
			cfg.Channel.Proxy.Subscribe.Endpoint = "http://localhost:8080"
			cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
				{
					Name: "room",
					ChannelOptions: configtypes.ChannelOptions{
						SubscribeForClient:            tc.subscribeForClient,
						SubscribeProxyEnabled:         tc.subscribeProxy,
						AllowChannelCompaction:        true,
						ClientPublishDebounceInterval: configtypes.Duration(50 * time.Millisecond),
					},
				},
			}
			cfgContainer, err := config.NewContainer(cfg)
			require.NoError(t, err)
			h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
			client := &tools.TestClientMock{
				IDFunc:      func() string { return "42" },
				UserIDFunc:  func() string { return "42" },
				ContextFunc: func() context.Context { return context.Background() },
			}
			proxyFunc := func(c proxy.Client, e centrifuge.SubscribeEvent, chOpts configtypes.ChannelOptions, pcd proxy.PerCallData) (centrifuge.SubscribeReply, proxy.SubscribeExtra, error) {
				return centrifuge.SubscribeReply{}, proxy.SubscribeExtra{}, nil
			}

			event := centrifuge.SubscribeEvent{
				Channel: "room:test",
			}
			if tc.useToken {
				event.Token = getSubscribeTokenHS("42", "room:test", 0)
			}
			reply, _, err := h.OnSubscribe(client, event, proxyFunc, nil)
			require.NoError(t, err)
			require.True(t, reply.Options.AllowChannelCompaction)
			require.Equal(t, 50*time.Millisecond, reply.Options.ClientPublishDebounceInterval)
		})
	}
}
