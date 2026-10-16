package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// noopSubscribeCallback answers a subscribe without committing it.
func noopSubscribeCallback(centrifuge.SubscribeReply, error) {}

// sidClaimsOf returns the claims of a token whose sid claim is sid.
func sidClaimsOf(sid string) filtertemplate.Claims {
	return filtertemplate.Claims{Paths: []string{"sid"}, Values: []string{sid}}
}

// TestServerTagsFilterTemplateConnectionUsesStoredUser checks the values of a render take the user stored with the
// claims: a connection whose Core user already moved, and whose values are not stored yet, renders with one identity.
func TestServerTagsFilterTemplateConnectionUsesStoredUser(t *testing.T) {
	stored := map[string]any{
		serverTagsFilterValuesKey: filtertemplate.Values{User: "a", Claims: sidClaimsOf("s-1")},
	}
	client := newRegistryTestClient("c1", "b", nil, stored)
	values := connectionFilterValues(client)
	require.Equal(t, "a", values.User)
	require.Equal(t, "s-1", values.Claims.Value("sid"))

	// A connection which stored no values renders with its user and no claims.
	legacy := newRegistryTestClient("c2", "b", nil, nil)
	values = connectionFilterValues(legacy)
	require.Equal(t, "b", values.User)
	require.Equal(t, "", values.Claims.Value("sid"))
}

// TestServerTagsFilterTemplateSubscribeAfterRebindUnsubscribes checks a subscribe whose values are changed by a rebind
// before the subscription is committed is unsubscribed, and one whose values stay the same is not.
func TestServerTagsFilterTemplateSubscribeAfterRebindUnsubscribes(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	reply := centrifuge.SubscribeReply{}

	// The commit stores the values of a rebind which ran after the render, so the channel is unsubscribed.
	rebound := newRegistryTestClient("c1", "12", nil, map[string]any{
		serverTagsFilterValuesKey: filtertemplate.Values{User: "12", Claims: sidClaimsOf("s-1")},
	})
	unsubscribed := map[string]centrifuge.Unsubscribe{}
	rebound.UnsubscribeFunc = func(ch string, unsubscribe ...centrifuge.Unsubscribe) {
		unsubscribed[ch] = unsubscribe[0]
	}
	env.h.answerSubscribe(rebound, "topic:room", reply, nil, func(centrifuge.SubscribeReply, error) {
		storage, release := rebound.AcquireStorage()
		storage[serverTagsFilterValuesKey] = filtertemplate.Values{User: "12", Claims: sidClaimsOf("s-2"), Client: "c1"}
		release(storage)
	})
	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), unsubscribed["topic:room"].Code)

	// Without a store during the commit the subscription stays.
	steady := newRegistryTestClient("c2", "12", nil, map[string]any{
		serverTagsFilterValuesKey: filtertemplate.Values{User: "12", Claims: sidClaimsOf("s-1")},
	})
	steadyUnsubscribed := map[string]centrifuge.Unsubscribe{}
	steady.UnsubscribeFunc = func(ch string, unsubscribe ...centrifuge.Unsubscribe) {
		steadyUnsubscribed[ch] = unsubscribe[0]
	}
	env.h.answerSubscribe(steady, "topic:room", reply, nil, noopSubscribeCallback)
	require.Empty(t, steadyUnsubscribed)
}

// TestServerTagsFilterTemplateFailedRebindRestoresValues checks a rebind which Core refuses leaves the values of the
// connection as they were, and no channel: the templated channel of the connection stays subscribed, since nothing it
// was subscribed with has changed.
func TestServerTagsFilterTemplateFailedRebindRestoresValues(t *testing.T) {
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
	state.channels["open:srv"] = centrifuge.ChannelContext{Source: subsource.ConnectionToken}
	state.rebindErr = errors.New("rebind refused")

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"sid": "s-1"}),
	}, nil)
	require.Error(t, err)
	require.Equal(t, "", state.user)
	// The values are the previous ones: the anonymous user, and no claims.
	values := connectionFilterValues(client)
	require.Equal(t, "", values.User)
	require.Equal(t, "", values.Claims.Value("sid"))
	require.NotContains(t, state.unsubscribed, "open:room")
	require.NotContains(t, state.unsubscribed, "open:srv")
	require.Contains(t, state.channels, "open:room")
	require.Contains(t, state.channels, "open:srv")
}

// TestServerTagsFilterTemplateRefusedConnectKeepsSingleConnection checks a connect which a template refuses leaves the
// other connection of its user connected: the single connection is enforced only once the connect is accepted.
func TestServerTagsFilterTemplateRefusedConnectKeepsSingleConnection(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.SingleConnection = true
	cfg.Channel.WithoutNamespace.Presence = true
	env := newTemplateEnv(t, cfg)
	_, firstTransport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))

	// The second token has no sid, which the template of topic needs under reject, so its connect is refused with 3507.
	secondTransport := newRecordingTransport()
	secondClient, closeFn, err := centrifuge.NewClient(context.Background(), env.node, secondTransport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(secondClient, &protocol.Command{
		Id: 1,
		Connect: &protocol.ConnectRequest{
			Token: connTokenOf(t, map[string]any{"subs": map[string]any{"topic:srv": map[string]any{}}}),
		},
	})
	require.Equal(t, centrifuge.DisconnectPermissionDenied.Code, secondTransport.waitDisconnect(t).Code)
	require.False(t, firstTransport.disconnected())
}

// TestServerTagsFilterTemplateTokenlessSkipSubscribesWithoutTemplate checks a connection without a token, which has no
// claims bound: under skip its subscription is made without the template, as skip says, and under reject it is refused.
func TestServerTagsFilterTemplateTokenlessSkipSubscribesWithoutTemplate(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.AllowAnonymousConnectWithoutToken = true
	for i := range cfg.Channel.Namespaces {
		cfg.Channel.Namespaces[i].ChannelOptions.SubscribeForAnonymous = true
	}
	env := newTemplateEnv(t, cfg)
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), env.node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	require.Nil(t, transport.waitReply(t, 1).Error)

	subscribeClient(t, client, transport, 2, "lenient:room", "")
	publishTagged(t, env.node, "lenient:room", `{"n":1}`, map[string]string{"session": "s-1"})
	waitPushed(t, transport, `{"n":1}`)

	sendRateLimitCommand(client, &protocol.Command{Id: 3, Subscribe: &protocol.SubscribeRequest{Channel: "topic:room"}})
	reply := transport.waitReply(t, 3)
	require.NotNil(t, reply.Error)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, reply.Error.Code)
	require.False(t, client.IsSubscribed("topic:room"))
	require.False(t, transport.disconnected())

	storage, release := client.AcquireStorage()
	unbound, _ := storage[serverTagsFilterUnboundKey].(bool)
	release(storage)
	require.True(t, unbound)
}
