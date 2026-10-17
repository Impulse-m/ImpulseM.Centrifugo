package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestRequireHTTPSubscribeProxyFilterKey checks the HTTP subscribe proxy answer carries the filter only under the
// server_tags_filter key of result: the decoder drops any other spelling silently, so the subscription is refused.
func TestRequireHTTPSubscribeProxyFilterKey(t *testing.T) {
	var answer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(answer))
	}))
	t.Cleanup(server.Close)

	subscribeProxy, err := proxy.NewHTTPSubscribeProxy(configtypes.Proxy{
		Endpoint: server.URL,
		Timeout:  configtypes.Duration(5 * time.Second),
	})
	require.NoError(t, err)
	proxyFn := proxy.NewSubscribeHandler(proxy.SubscribeHandlerConfig{
		Proxies: map[string]proxy.SubscribeProxy{"default": subscribeProxy},
	}).Handle()

	cfg := requireTestConfig()
	cfg.Channel.Proxy.Subscribe.Endpoint = server.URL
	cfg.Channel.Namespaces[0].SubscribeProxyEnabled = true
	cfg.Channel.Namespaces[0].SubscribeProxyName = "default"
	env := newTemplateEnv(t, cfg)
	client := requireMock()
	// The proxy call reads the context and the transport of the client.
	client.ContextFunc = context.Background
	client.TransportFunc = func() centrifuge.TransportInfo {
		return tools.NewTestTransport()
	}
	event := centrifuge.SubscribeEvent{Channel: "req:room"}

	refused := []string{
		`{"result":{"serverTagsFilter":{"key":"type","cmp":"eq","val":"a"}}}`,
		`{"serverTagsFilter":{"key":"type","cmp":"eq","val":"a"}}`,
		`{"result":{"server_tags_filter":null}}`,
		`{"result":{}}`,
	}
	for _, body := range refused {
		answer = body
		_, err := answerSubscribeOf(t, env.h, client, event, proxyFn)
		require.Equal(t, centrifuge.ErrorPermissionDenied, err, body)
	}

	answer = `{"result":{"server_tags_filter":{"key":"type","cmp":"eq","val":"a"}}}`
	reply, err := answerSubscribeOf(t, env.h, client, event, proxyFn)
	require.NoError(t, err)
	require.NotNil(t, reply.Options.ServerTagsFilter)
}

// TestRequireWithoutNamespaceSubscribe checks a subscription to a channel of channel.without_namespace, which has the
// option, is refused without a filter.
func TestRequireWithoutNamespaceSubscribe(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.WithoutNamespace.SubscribeForClient = true
	cfg.Channel.WithoutNamespace.RequireServerTagsFilter = true
	env := newTemplateEnv(t, cfg)

	_, err := answerSubscribeOf(t, env.h, requireMock(), centrifuge.SubscribeEvent{Channel: "room"}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
}

// TestRequireMapSubscription checks a map subscription without a filter is refused, and the same subscription with a
// subscription token which carries the filter succeeds.
func TestRequireMapSubscription(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "mapreq",
			ChannelOptions: configtypes.ChannelOptions{
				SubscriptionType:        "map",
				SubscribeForClient:      true,
				RequireServerTagsFilter: true,
				Map: configtypes.MapConfig{
					Mode:   "ephemeral",
					KeyTTL: configtypes.Duration(time.Minute),
				},
			},
		},
	}
	env := newTemplateEnv(t, cfg)
	event := centrifuge.SubscribeEvent{Channel: "mapreq:room", Type: centrifuge.SubscriptionTypeMap}

	_, err := answerSubscribeOf(t, env.h, requireMock(), event, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)

	event.Token = getSubscribeTokenHSWithServerTagsFilter("12", "mapreq:room", testServerTagsFilter())
	_, err = answerSubscribeOf(t, env.h, requireMock(), event, nil)
	require.NoError(t, err)
}

// TestRequireHistoryRefusedWhenUnsubscribed checks client history is refused on a namespace which requires a filter also
// for a client which is not subscribed, the case allow_history_for_client opens.
func TestRequireHistoryRefusedWhenUnsubscribed(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Channel.Namespaces[0].HistoryForClient = true
	env := newTemplateEnv(t, cfg)

	_, err := env.h.OnHistory(requireMock(), centrifuge.HistoryEvent{Channel: "req:room"})
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
}

// TestRequireReboundFallbackAfterReload checks a reload which turns the option on between the pre-check of a rebind and
// its subscribe is caught by the fallback: the rebind is applied and the connection is disconnected, which releases it.
func TestRequireReboundFallbackAfterReload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "open", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}},
	}
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, cfgContainer := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	reloaded := cfg
	reloaded.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "open", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true, RequireServerTagsFilter: true}},
	}
	client, state := newRebindTestClient("", "anonymous")
	state.beforeApply = func(_ centrifuge.RebindOptions) {
		require.NoError(t, cfgContainer.Reload(reloaded))
	}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"open:room": map[string]any{}},
		}),
	}, nil)
	require.Equal(t, centrifuge.DisconnectPermissionDenied, err)
	require.Equal(t, "42", state.user)
	require.NotContains(t, state.subscribed, "open:room")
}

// TestRequireReboundKeepsHeldChannel checks a rebound token whose subs entry has no filter for a channel the connection
// already holds is accepted: the channel is not subscribed again, so it is not refused, and the held subscription is kept.
func TestRequireReboundKeepsHeldChannel(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	state.channels["req:room"] = centrifuge.ChannelContext{Source: subsource.ClientAllowed}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{}},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.Contains(t, state.channels, "req:room")
	require.EqualValues(t, subsource.ClientAllowed, state.channels["req:room"].Source)
}

// TestRequireAnonymousRebindRefusesHeldChannel checks a rebind to an anonymous identity whose subs entry has no filter for a
// channel the connection holds from its token is refused before the connection moves: the rebind removes that channel and
// subscribes it again from the token, so the subscription would be refused after the move. With the filter the rebind applies.
func TestRequireAnonymousRebindRefusesHeldChannel(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true, AllowDowngrade: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("42", "authenticated")
	state.channels["req:room"] = centrifuge.ChannelContext{Source: subsource.ConnectionToken}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{}},
		}),
	}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
	require.Equal(t, "42", state.user)
	require.Empty(t, state.rebinds)
	require.Empty(t, state.unsubscribed)
	require.Contains(t, state.channels, "req:room")

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"req:room": map[string]any{"server_tags_filter": testServerTagsFilter()}},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "", state.user)
	require.Contains(t, state.subscribed, "req:room")
}

// TestRequireReboundUnsubscribesDroppedChannel checks a held uni-connect channel of a template namespace which a rebind drops
// for the require option is unsubscribed. The rebind keeps the template values, so the template does not unsubscribe the
// channel, and a channel the connection still held would stay on it without a filter.
func TestRequireReboundUnsubscribesDroppedChannel(t *testing.T) {
	cfg := requireTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	// The pending connection of user 42 stored its values with the user, and holds "tpl:room" which connect subscribed.
	client, state := newRebindTestClientWithToken("", "42", "pending_2fa")
	storeFilterValues(client, filtertemplate.Values{User: "42"})
	storage, release := client.AcquireStorage()
	storage[serverTagsFilterServerSideKey] = map[string]centrifuge.SubscribeOptions{
		"tpl:room": {Source: subsource.UniConnect},
	}
	release(storage)
	state.channels["tpl:room"] = centrifuge.ChannelContext{Source: subsource.UniConnect}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.NotContains(t, state.channels, "tpl:room")
	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["tpl:room"].Code)
}

// TestRequireConnectKeepsSingleConnection checks a connect which the option refuses leaves the other connection of its user
// connected, without any template: the single connection is enforced only once the connect is accepted.
func TestRequireConnectKeepsSingleConnection(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name:           "req",
			ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true, RequireServerTagsFilter: true},
		},
	}
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.SingleConnection = true
	cfg.Channel.WithoutNamespace.Presence = true
	env := newTemplateEnv(t, cfg)
	_, firstTransport := connectWithConnToken(t, env.node, connTokenOf(t, nil))
	personalChannel := env.cfgContainer.PersonalChannel("12")
	require.Eventually(t, func() bool {
		stats, err := env.node.PresenceStats(personalChannel)
		return err == nil && stats.NumClients == 1
	}, 5*time.Second, time.Millisecond)

	// The channels claim of the second token has no filter for req, so its connect is refused with 3507.
	secondTransport := newRecordingTransport()
	secondClient, closeFn, err := centrifuge.NewClient(context.Background(), env.node, secondTransport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(secondClient, &protocol.Command{
		Id: 1,
		Connect: &protocol.ConnectRequest{
			Token: connTokenOf(t, map[string]any{"channels": []string{"req:room"}}),
		},
	})
	require.Equal(t, centrifuge.DisconnectPermissionDenied.Code, secondTransport.waitDisconnect(t).Code)
	// The single connection disconnect of the first connection is asynchronous, so it is watched for a while.
	require.Never(t, firstTransport.disconnected, 300*time.Millisecond, 10*time.Millisecond)
}
