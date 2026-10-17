package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// requireTestConfig returns the config of the require option tests: "req" requires a filter and subscribes clients,
// "open" subscribes clients without the option, and "tpl" has a session template under skip which requires a filter.
func requireTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "req",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient:      true,
				RequireServerTagsFilter: true,
			},
		},
		{
			Name: "open",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
			},
		},
		{
			Name: "tpl",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient:      true,
				RequireServerTagsFilter: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter:         configtypes.TagsFilterNode{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
					OnMissingValue: "skip",
				},
			},
		},
	}
	return cfg
}

// requireMock returns a client of user 12 for the handler calls which do not use a connection.
func requireMock() *tools.TestClientMock {
	return &tools.TestClientMock{
		IDFunc:     func() string { return "c1" },
		UserIDFunc: func() string { return "12" },
	}
}

// answerSubscribeOf runs the subscribe of e as the subscribe callback answers it, with the subscribe proxy handler
// proxyFn. It returns the reply and the error the client gets.
func answerSubscribeOf(
	t *testing.T,
	h *Handler,
	c Client,
	e centrifuge.SubscribeEvent,
	proxyFn proxy.SubscribeHandlerFunc,
) (centrifuge.SubscribeReply, error) {
	t.Helper()
	reply, _, err := h.OnSubscribe(c, e, proxyFn, nil)
	var answered centrifuge.SubscribeReply
	var answeredErr error
	h.answerSubscribe(c, e.Channel, reply, err, func(r centrifuge.SubscribeReply, err error) {
		answered = r
		answeredErr = err
	})
	return answered, answeredErr
}

// TestRequireSubscriptionTokenWithoutFilter checks a subscription token without the server tags filter claim is refused
// in a namespace which requires one, and the same token with the claim subscribes.
func TestRequireSubscriptionTokenWithoutFilter(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, nil))

	sendRateLimitCommand(client, &protocol.Command{
		Id:        2,
		Subscribe: &protocol.SubscribeRequest{Channel: "req:room", Token: getSubscribeTokenHSWithServerTagsFilter("12", "req:room", nil)},
	})
	reply := transport.waitReply(t, 2)
	require.NotNil(t, reply.Error)
	require.Equal(t, uint32(centrifuge.ErrorPermissionDenied.Code), reply.Error.Code)
	require.False(t, client.IsSubscribed("req:room"))

	sendRateLimitCommand(client, &protocol.Command{
		Id:        3,
		Subscribe: &protocol.SubscribeRequest{Channel: "req:room", Token: getSubscribeTokenHSWithServerTagsFilter("12", "req:room", testServerTagsFilter())},
	})
	reply = transport.waitReply(t, 3)
	require.Nil(t, reply.Error)
	require.True(t, client.IsSubscribed("req:room"))
}

// TestRequireSubscribeProxyAnswer checks the subscribe proxy answer without a filter is refused, with a filter subscribes,
// and that a proxy error is passed through unchanged.
func TestRequireSubscribeProxyAnswer(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Channel.Proxy.Subscribe.Endpoint = "http://localhost:8080"
	cfg.Channel.Namespaces[0].SubscribeProxyEnabled = true
	cfg.Channel.Namespaces[0].SubscribeProxyName = "default"
	env := newTemplateEnv(t, cfg)
	client := requireMock()
	event := centrifuge.SubscribeEvent{Channel: "req:room"}

	_, err := answerSubscribeOf(t, env.h, client, event, func(proxy.Client, centrifuge.SubscribeEvent, configtypes.ChannelOptions, proxy.PerCallData) (centrifuge.SubscribeReply, proxy.SubscribeExtra, error) {
		return centrifuge.SubscribeReply{}, proxy.SubscribeExtra{}, nil
	})
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)

	reply, err := answerSubscribeOf(t, env.h, client, event, func(proxy.Client, centrifuge.SubscribeEvent, configtypes.ChannelOptions, proxy.PerCallData) (centrifuge.SubscribeReply, proxy.SubscribeExtra, error) {
		return centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{ServerTagsFilter: testServerTagsFilter()}}, proxy.SubscribeExtra{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, testServerTagsFilter(), reply.Options.ServerTagsFilter)

	_, err = answerSubscribeOf(t, env.h, client, event, func(proxy.Client, centrifuge.SubscribeEvent, configtypes.ChannelOptions, proxy.PerCallData) (centrifuge.SubscribeReply, proxy.SubscribeExtra, error) {
		return centrifuge.SubscribeReply{}, proxy.SubscribeExtra{}, centrifuge.ErrorInternal
	})
	require.Equal(t, centrifuge.ErrorInternal, err)
}

// TestRequireUserLimitedChannel checks a user-limited channel, which has no filter, is refused in a namespace which requires one.
func TestRequireUserLimitedChannel(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Channel.UserBoundary = "#"
	cfg.Channel.Namespaces[0].UserLimitedChannels = true
	env := newTemplateEnv(t, cfg)
	_, err := answerSubscribeOf(t, env.h, requireMock(), centrifuge.SubscribeEvent{Channel: "req:room#12"}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
}

// TestRequireAllowSubscribeForClient checks allow_subscribe_for_client channels are refused in a namespace which requires a
// filter, and allowed in one which does not.
func TestRequireAllowSubscribeForClient(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())
	_, err := answerSubscribeOf(t, env.h, requireMock(), centrifuge.SubscribeEvent{Channel: "req:room"}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)

	_, err = answerSubscribeOf(t, env.h, requireMock(), centrifuge.SubscribeEvent{Channel: "open:room"}, nil)
	require.NoError(t, err)
}

// TestRequireClientInsecure checks a client subscription with client.insecure is refused in a namespace which requires a filter.
func TestRequireClientInsecure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name:           "req",
			ChannelOptions: configtypes.ChannelOptions{RequireServerTagsFilter: true},
		},
	}
	env := newTemplateEnv(t, cfg)
	_, err := answerSubscribeOf(t, env.h, requireMock(), centrifuge.SubscribeEvent{Channel: "req:room"}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
}

// TestRequireConnectionTokenChannels checks a connection token whose channels claim, or whose subs entry, has no filter
// refuses the connection with DisconnectPermissionDenied, and a subs entry with the filter connects.
func TestRequireConnectionTokenChannels(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())
	ctx := context.Background()

	_, err := env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    connTokenOf(t, map[string]any{"channels": []string{"req:room"}}),
	}, nil, false)
	require.Equal(t, centrifuge.DisconnectPermissionDenied, err)

	_, err = env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    connTokenOf(t, map[string]any{"subs": map[string]any{"req:room": map[string]any{"server_tags_filter": nil}}}),
	}, nil, false)
	require.Equal(t, centrifuge.DisconnectPermissionDenied, err)

	reply, err := env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{
		ClientID: "c1",
		Token: connTokenOf(t, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{"server_tags_filter": testServerTagsFilter()}},
		}),
	}, nil, false)
	require.NoError(t, err)
	require.Contains(t, reply.Subscriptions, "req:room")
}

// TestRequireConnectProxySubscriptions checks a connect proxy channel without a filter refuses the connection, and a subs
// entry with the filter connects.
func TestRequireConnectProxySubscriptions(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())
	ctx := context.Background()
	withoutFilter := func(context.Context, centrifuge.ConnectEvent) (centrifuge.ConnectReply, proxy.ConnectExtra, error) {
		return centrifuge.ConnectReply{
			Credentials:   &centrifuge.Credentials{UserID: "12"},
			Subscriptions: map[string]centrifuge.SubscribeOptions{"req:room": {Source: 2}},
		}, proxy.ConnectExtra{}, nil
	}
	_, err := env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{ClientID: "c1"}, withoutFilter, false)
	require.Equal(t, centrifuge.DisconnectPermissionDenied, err)

	withFilter := func(context.Context, centrifuge.ConnectEvent) (centrifuge.ConnectReply, proxy.ConnectExtra, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "12"},
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"req:room": {Source: 2, ServerTagsFilter: testServerTagsFilter()},
			},
		}, proxy.ConnectExtra{}, nil
	}
	reply, err := env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{ClientID: "c1"}, withFilter, false)
	require.NoError(t, err)
	require.Contains(t, reply.Subscriptions, "req:room")
}

// TestRequireClientChannelDroppedAtConnect checks a channel the client asks for at connect, which has no filter, is dropped
// from the connection, which is made. A token subs entry for the same channel with the filter is kept.
func TestRequireClientChannelDroppedAtConnect(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Client.AllowAnonymousConnectWithoutToken = true
	cfg.Channel.Namespaces[0].SubscribeForAnonymous = true
	env := newTemplateEnv(t, cfg)
	ctx := context.Background()

	reply, err := env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{
		ClientID: "c1",
		Channels: []string{"req:room"},
	}, nil, false)
	require.NoError(t, err)
	require.NotContains(t, reply.Subscriptions, "req:room")

	reply, err = env.h.OnClientConnecting(ctx, centrifuge.ConnectEvent{
		ClientID: "c1",
		Token: connTokenOf(t, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{"server_tags_filter": testServerTagsFilter()}},
		}),
		Channels: []string{"req:room"},
	}, nil, false)
	require.NoError(t, err)
	require.Contains(t, reply.Subscriptions, "req:room")
	require.Equal(t, testServerTagsFilter(), reply.Subscriptions["req:room"].ServerTagsFilter)
}

// TestRequireTemplateFilterCounts checks a filter which a template produces counts: with a claim the template renders a
// filter and the subscription is made; under skip, a connection without the claim has no filter and is refused.
func TestRequireTemplateFilterCounts(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())

	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	sendRateLimitCommand(client, &protocol.Command{
		Id:        2,
		Subscribe: &protocol.SubscribeRequest{Channel: "tpl:room"},
	})
	reply := transport.waitReply(t, 2)
	require.Nil(t, reply.Error)
	require.True(t, client.IsSubscribed("tpl:room"))

	client, transport = connectWithConnToken(t, env.node, connTokenOf(t, nil))
	sendRateLimitCommand(client, &protocol.Command{
		Id:        2,
		Subscribe: &protocol.SubscribeRequest{Channel: "tpl:room"},
	})
	reply = transport.waitReply(t, 2)
	require.NotNil(t, reply.Error)
	require.False(t, client.IsSubscribed("tpl:room"))
}

// TestRequireHistoryRefused checks client history is refused on a namespace which requires a filter.
func TestRequireHistoryRefused(t *testing.T) {
	env := newTemplateEnv(t, requireTestConfig())
	_, err := env.h.OnHistory(requireMock(), centrifuge.HistoryEvent{Channel: "req:room"})
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
}

// TestRequireOffByDefault checks the option is off by default: the checks do not allocate, a filterless subscription and a
// connection token channels claim of a namespace without the option are not refused.
func TestRequireOffByDefault(t *testing.T) {
	env := newTemplateEnv(t, config.DefaultConfig())
	require.False(t, env.cfgContainer.ConfigRef().RequiresServerTagsFilter())

	reply := &centrifuge.SubscribeReply{}
	allocs := testing.AllocsPerRun(1000, func() {
		_ = env.h.requireSubscribeFilter(nil, "room", reply, nil)
	})
	require.Equal(t, float64(0), allocs)

	subs := map[string]centrifuge.SubscribeOptions{}
	allocs = testing.AllocsPerRun(1000, func() {
		_ = env.h.requireConnectFilters("c1", nil, subs, nil, false)
	})
	require.Equal(t, float64(0), allocs)

	off := newTemplateEnv(t, requireTestConfig())
	_, err := answerSubscribeOf(t, off.h, requireMock(), centrifuge.SubscribeEvent{Channel: "open:room"}, nil)
	require.NoError(t, err)
	// connectWithConnToken requires the connect to succeed.
	client, _ := connectWithConnToken(t, off.node, connTokenOf(t, map[string]any{"channels": []string{"open:room"}}))
	require.NotNil(t, client)
}

// TestRequireReloadTurnsOnForNewSubscriptions checks a reload which turns the option on leaves a live subscription as it
// is, and refuses a new subscription without a filter.
func TestRequireReloadTurnsOnForNewSubscriptions(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "open", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}},
	}
	env := newTemplateEnv(t, cfg)
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, nil))
	sendRateLimitCommand(client, &protocol.Command{
		Id:        2,
		Subscribe: &protocol.SubscribeRequest{Channel: "open:room", Token: getSubscribeTokenHSWithServerTagsFilter("12", "open:room", nil)},
	})
	require.Nil(t, transport.waitReply(t, 2).Error)

	reloaded := config.DefaultConfig()
	reloaded.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "open", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true, RequireServerTagsFilter: true}},
	}
	require.NoError(t, env.cfgContainer.Reload(reloaded))
	require.True(t, client.IsSubscribed("open:room"))

	// The data is JSON, as the JSON protocol of the transport carries it.
	publishTagged(t, env.node, "open:room", `{"n":4}`, nil)
	waitPushed(t, transport, `{"n":4}`)

	sendRateLimitCommand(client, &protocol.Command{
		Id:        3,
		Subscribe: &protocol.SubscribeRequest{Channel: "open:other", Token: getSubscribeTokenHSWithServerTagsFilter("12", "open:other", nil)},
	})
	require.NotNil(t, transport.waitReply(t, 3).Error)
}

// TestRequireRebindRefusedBeforeMove checks a rebind to another user whose channels claim has no filter for a namespace which
// requires one is refused as a refresh error before the connection moves, and the same rebind with the filter subscribes.
func TestRequireRebindRefusedBeforeMove(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"channels": []string{"req:room"}}),
	}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
	require.Equal(t, "", state.user)
	require.Empty(t, state.rebinds)
	require.Empty(t, state.subscribed)

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{"server_tags_filter": testServerTagsFilter()}},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.Contains(t, state.subscribed, "req:room")
}
