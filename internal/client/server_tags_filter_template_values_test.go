package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestServerTagsFilterTemplateRebindUserChangeUnsubscribes checks a rebind which gives a connection without a user a
// user unsubscribes its channels with a template, although the claims are the same: the template renders another user.
func TestServerTagsFilterTemplateRebindUserChangeUnsubscribes(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{
		Name: "open",
		ChannelOptions: configtypes.ChannelOptions{
			SubscribeForAnonymous: true,
			ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
				Filter:         configtypes.TagsFilterNode{"key": "audience", "cmp": "in", "vals": []any{"all", "user:{{user}}"}},
				OnMissingValue: "skip",
			},
		},
	})
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	state.channels["open:room"] = centrifuge.ChannelContext{Source: subsource.ClientAllowed}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["open:room"].Code)
}

// TestServerTagsFilterTemplateRebindRefusalKeepsIdentity checks a rebind whose token subscription a template refuses is
// refused before the connection moves: the identity, the values and the channels stay as they were.
func TestServerTagsFilterTemplateRebindRefusalKeepsIdentity(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	state.channels["lenient:room"] = centrifuge.ChannelContext{Source: subsource.ClientAllowed}

	// The token has no sid claim, which the template of chat needs under reject.
	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"subs": map[string]any{"chat:srv": map[string]any{}},
		}),
	}, nil)
	require.Equal(t, centrifuge.ErrorPermissionDenied, err)
	require.Empty(t, state.rebinds)
	require.Equal(t, "", state.user)
	require.Empty(t, state.unsubscribed)
	require.Contains(t, state.channels, "lenient:room")
}

// TestServerTagsFilterTemplateRefreshWithoutClaims checks a refresh of a connection without a token keeps its
// connection when the refresh token has no value for the template claims, and disconnects it when the token has one,
// since the values changed.
func TestServerTagsFilterTemplateRefreshWithoutClaims(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.AllowAnonymousConnectWithoutToken = true
	env := newTemplateEnv(t, cfg)
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), env.node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	require.Nil(t, transport.waitReply(t, 1).Error)

	// Empty and absent claims are the same missing values, so the connection is kept.
	sendRateLimitCommand(client, &protocol.Command{
		Id:      2,
		Refresh: &protocol.RefreshRequest{Token: getConnTokenWithClaims(t, map[string]any{"sid": ""})},
	})
	require.Nil(t, transport.waitReply(t, 2).Error)
	require.False(t, transport.disconnected())

	// A value where there was none changes the values, so the connection is disconnected.
	sendRateLimitCommand(client, &protocol.Command{
		Id:      3,
		Refresh: &protocol.RefreshRequest{Token: getConnTokenWithClaims(t, map[string]any{"sid": "s-1"})},
	})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectInsufficientState.Code, disconnect.Code)
}

// TestServerTagsFilterTemplateReloadChangesClaimPaths checks a refresh after a reload which changes the union of the
// claim paths disconnects a connection whose claims differ by name. The values line up by position, so a comparison
// by position would keep the connection.
func TestServerTagsFilterTemplateReloadChangesClaimPaths(t *testing.T) {
	first := config.DefaultConfig()
	first.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "chat",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "org", "cmp": "eq", "val": "{{claims.org}}"},
				},
			},
		},
		{
			Name: "lenient",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter:         configtypes.TagsFilterNode{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
					OnMissingValue: "skip",
				},
			},
		},
	}
	env := newTemplateEnv(t, first)
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"org": "o1", "sid": "s1"}))
	require.NotNil(t, client)

	second := config.DefaultConfig()
	second.Channel.Namespaces = configtypes.ChannelNamespaces{
		first.Channel.Namespaces[0],
		{
			Name: "lenient",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter:         configtypes.TagsFilterNode{"key": "tenant", "cmp": "neq", "val": "{{claims.tenant}}"},
					OnMissingValue: "skip",
				},
			},
		},
	}
	require.NoError(t, env.cfgContainer.Reload(second))

	// The union is now [org tenant], and the values by position are [o1 s1] as before. By name the session claim is gone.
	sendRateLimitCommand(client, &protocol.Command{
		Id:      2,
		Refresh: &protocol.RefreshRequest{Token: connTokenOf(t, map[string]any{"org": "o1", "tenant": "s1"})},
	})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectInsufficientState.Code, disconnect.Code)
}

// TestServerTagsFilterTemplateRebindKeepsUntemplatedServerSide checks a rebind with other values keeps a uni-connect
// channel which the connection got before a template was configured. Connect subscribed it without a template, so its
// filter does not depend on the values, and it is not among the server-side subscriptions stored with a template.
func TestServerTagsFilterTemplateRebindKeepsUntemplatedServerSide(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name:           "lenient",
			ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true},
		},
	}
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, cfgContainer := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	state.channels["lenient:uni"] = centrifuge.ChannelContext{Source: subsource.UniConnect}

	// The templates are configured after the connect of the connection.
	reloaded := templateTestConfig()
	reloaded.Client.Rebind = configtypes.Rebind{Enabled: true}
	reloaded.RPC.Registry.AuthClassClaim = rebindTestClaim
	require.NoError(t, cfgContainer.Reload(reloaded))

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"sid": "s-1"}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.NotContains(t, state.unsubscribed, "lenient:uni")
	require.Contains(t, state.channels, "lenient:uni")
}

// TestServerTagsFilterTemplateRebindStoresTokenFilters checks a rebind with other values stores the filters of the
// connection token subscriptions it subscribes, so that a refresh with the same claims compares them with the filters
// the channels have, and keeps the connection.
func TestServerTagsFilterTemplateRebindStoresTokenFilters(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"sid":  "s-1",
			"subs": map[string]any{"lenient:srv": map[string]any{}},
		}),
	}, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(sessionFilter("s-1"), state.subscribed["lenient:srv"].ServerTagsFilter))

	storage, release := client.AcquireStorage()
	filters, _ := storage[connectionTokenFiltersKey].(map[string]*protocol.FilterNode)
	release(storage)
	require.True(t, proto.Equal(sessionFilter("s-1"), filters["lenient:srv"]), "got %v", filters)
}

// TestServerTagsFilterTemplateRebindWhileTemplatesOff checks a connection which stored its template values keeps them on
// a rebind made while the config has no templates. A later reload which adds the template renders the user the connection
// has now, not the empty user it had when the templates were switched off.
func TestServerTagsFilterTemplateRebindWhileTemplatesOff(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "owned",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "owner", "cmp": "eq", "val": "{{user}}"},
				},
			},
		},
	}
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, cfgContainer := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	// The anonymous connection stored its values on connect, with no user.
	client, state := newRebindTestClient("", "anonymous")
	storeFilterValues(client, filtertemplate.Values{})

	// The templates are switched off, and the connection rebinds to a user.
	off := config.DefaultConfig()
	off.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "owned", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}},
	}
	off.Client.Rebind = configtypes.Rebind{Enabled: true}
	off.RPC.Registry.AuthClassClaim = rebindTestClaim
	require.NoError(t, cfgContainer.Reload(off))
	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "u2", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "u2", state.user)

	// The templates come back, and a render uses the user the connection has now.
	require.NoError(t, cfgContainer.Reload(cfg))
	values := connectionFilterValues(client)
	require.Equal(t, "u2", values.User)
	rendered, err := h.templateOf("owned:room").Render(values)
	require.NoError(t, err)
	require.Equal(t, "u2", rendered.Val)
}
