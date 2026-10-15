package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
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
