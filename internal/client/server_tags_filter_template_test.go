package client

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// templateEnv is a node with a handler which has the server tags filter templates of the config.
type templateEnv struct {
	node         *centrifuge.Node
	h            *Handler
	cfgContainer *config.Container
}

func newTemplateEnv(t *testing.T, cfg config.Config) *templateEnv {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return &templateEnv{node: node, h: h, cfgContainer: cfgContainer}
}

// sessionTemplateNamespaces returns the config of a namespace which subscribes clients with a session template
// under reject ("chat") and under skip ("lenient").
func sessionTemplateNamespaces() configtypes.ChannelNamespaces {
	return configtypes.ChannelNamespaces{
		{
			Name: "chat",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
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
}

func templateTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = sessionTemplateNamespaces()
	return cfg
}

// connTokenOf returns a connection token of user 12 with the given extra claims.
func connTokenOf(t *testing.T, extra map[string]any) string {
	t.Helper()
	claims := map[string]any{"sub": "12"}
	for key, value := range extra {
		claims[key] = value
	}
	return getConnTokenWithClaims(t, claims)
}

// publishTagged publishes data to channel with the tags, or without tags when tags is nil.
func publishTagged(t *testing.T, node *centrifuge.Node, channel string, data string, tags map[string]string) {
	t.Helper()
	var opts []centrifuge.PublishOption
	if tags != nil {
		opts = append(opts, centrifuge.WithTags(tags))
	}
	_, err := node.Publish(channel, []byte(data), opts...)
	require.NoError(t, err)
}

// pushedData returns the data of the publications pushed to the transport, in order.
func (t *recordingTransport) pushedData() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var data []string
	for _, reply := range t.replies {
		if reply.Push != nil && reply.Push.Pub != nil {
			data = append(data, string(reply.Push.Pub.Data))
		}
	}
	return data
}

// waitPushed waits until the publication with data is pushed to the transport. Publications of a channel arrive
// in order, so the publications before it, which were not delivered, are not pushed either once it is.
func waitPushed(t *testing.T, transport *recordingTransport, data string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.Contains(transport.pushedData(), data)
	}, 5*time.Second, time.Millisecond)
}

// subscribeClient subscribes the client to channel with the subscribe command id, and checks it succeeded.
func subscribeClient(t *testing.T, client *centrifuge.Client, transport *recordingTransport, id uint32, channel string, token string) {
	t.Helper()
	sendRateLimitCommand(client, &protocol.Command{
		Id:        id,
		Subscribe: &protocol.SubscribeRequest{Channel: channel, Token: token},
	})
	reply := transport.waitReply(t, id)
	require.Nil(t, reply.Error)
}

// sessionFilter returns the filter the session template renders to for session.
func sessionFilter(session string) *protocol.FilterNode {
	return &protocol.FilterNode{Key: "session", Cmp: "neq", Val: session}
}

// TestServerTagsFilterTemplateClientSubscription checks a client subscription in a namespace with a template: the
// publications of its own session are dropped, those of another session and untagged ones are delivered.
func TestServerTagsFilterTemplateClientSubscription(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	subscribeClient(t, client, transport, 2, "chat:room", "")

	publishTagged(t, env.node, "chat:room", `{"n":1}`, map[string]string{"session": "s-1"})
	publishTagged(t, env.node, "chat:room", `{"n":2}`, map[string]string{"session": "s-2"})
	publishTagged(t, env.node, "chat:room", `{"n":3}`, nil)
	waitPushed(t, transport, `{"n":3}`)
	require.Equal(t, []string{`{"n":2}`, `{"n":3}`}, transport.pushedData())
}

// TestServerTagsFilterTemplateWithSubscriptionTokenFilter checks a subscription token filter is combined with the
// template: a publication must satisfy both.
func TestServerTagsFilterTemplateWithSubscriptionTokenFilter(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	subscribeClient(t, client, transport, 2, "chat:room", getSubscribeTokenHSWithServerTagsFilter("12", "chat:room", testServerTagsFilter()))

	// Matches the explicit filter, but has the session of the connection.
	publishTagged(t, env.node, "chat:room", `{"n":1}`, map[string]string{"type": "a", "tier": "x", "session": "s-1"})
	// Has another session, but misses the explicit filter.
	publishTagged(t, env.node, "chat:room", `{"n":2}`, map[string]string{"type": "b", "tier": "x", "session": "s-2"})
	publishTagged(t, env.node, "chat:room", `{"n":3}`, map[string]string{"type": "a", "tier": "x", "session": "s-2"})
	waitPushed(t, transport, `{"n":3}`)
	require.Equal(t, []string{`{"n":3}`}, transport.pushedData())
}

// TestServerTagsFilterTemplateSubscribeProxyFilter checks the filter of a subscribe proxy reply, which is what the
// subscription gets from the proxy, is combined with the template of the channel.
func TestServerTagsFilterTemplateSubscribeProxyFilter(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	client, _ := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))

	reply := centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{ServerTagsFilter: testServerTagsFilter()}}
	var answered centrifuge.SubscribeReply
	var answeredErr error
	env.h.answerSubscribe(client, "chat:room", reply, nil, func(r centrifuge.SubscribeReply, err error) {
		answered = r
		answeredErr = err
	})
	require.NoError(t, answeredErr)
	opts := answered.Options
	want := &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			sessionFilter("s-1"),
			testServerTagsFilter(),
		},
	}
	require.True(t, proto.Equal(want, opts.ServerTagsFilter), "got %v", opts.ServerTagsFilter)
}

// TestServerTagsFilterTemplateMissingClaim checks a missing claim: reject refuses the subscription with 103, skip
// subscribes without the template.
func TestServerTagsFilterTemplateMissingClaim(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())

	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, nil))
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "chat:room"}})
	reply := transport.waitReply(t, 2)
	require.NotNil(t, reply.Error)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, reply.Error.Code)
	require.False(t, client.IsSubscribed("chat:room"))

	client, transport = connectWithConnToken(t, env.node, connTokenOf(t, nil))
	subscribeClient(t, client, transport, 2, "lenient:room", "")
	publishTagged(t, env.node, "lenient:room", `{"n":1}`, map[string]string{"session": "s-1"})
	waitPushed(t, transport, `{"n":1}`)
}

// TestServerTagsFilterTemplateConnectionSubscriptions checks server-side subscriptions of a connection token: they
// are filtered with the session of the connection, and a missing claim disconnects under reject, or connects
// unfiltered under skip.
func TestServerTagsFilterTemplateConnectionSubscriptions(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	subs := map[string]any{"subs": map[string]any{"chat:srv": map[string]any{}}}

	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, mergeClaims(subs, map[string]any{"sid": "s-1"})))
	require.True(t, client.IsSubscribed("chat:srv"))
	publishTagged(t, env.node, "chat:srv", `{"n":1}`, map[string]string{"session": "s-1"})
	publishTagged(t, env.node, "chat:srv", `{"n":2}`, map[string]string{"session": "s-2"})
	waitPushed(t, transport, `{"n":2}`)
	require.Equal(t, []string{`{"n":2}`}, transport.pushedData())

	// A missing claim under reject is a disconnect at connect.
	rejected := newRecordingTransport()
	rejectedClient, closeFn, err := centrifuge.NewClient(context.Background(), env.node, rejected)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(rejectedClient, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Token: connTokenOf(t, mergeClaims(subs, nil))}})
	disconnect := rejected.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectPermissionDenied.Code, disconnect.Code)

	lenientSubs := map[string]any{"subs": map[string]any{"lenient:srv": map[string]any{}}}
	client, transport = connectWithConnToken(t, env.node, connTokenOf(t, mergeClaims(lenientSubs, nil)))
	require.True(t, client.IsSubscribed("lenient:srv"))
	publishTagged(t, env.node, "lenient:srv", `{"n":3}`, map[string]string{"session": "s-1"})
	waitPushed(t, transport, `{"n":3}`)
}

// mergeClaims returns a copy of base with the extra claims added; a nil extra returns a copy of base.
func mergeClaims(base map[string]any, extra map[string]any) map[string]any {
	merged := map[string]any{}
	maps.Copy(merged, base)
	maps.Copy(merged, extra)
	return merged
}

// TestServerTagsFilterTemplateRefreshKeepsSubscriptions checks a connection refresh with the same claims keeps the
// connection and its subscriptions, with one reply and no further traffic, and a refresh with other claims
// disconnects with insufficient state.
func TestServerTagsFilterTemplateRefreshKeepsSubscriptions(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	subs := map[string]any{"subs": map[string]any{"chat:srv": map[string]any{}}}
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, mergeClaims(subs, map[string]any{"sid": "s-1"})))
	require.True(t, client.IsSubscribed("chat:srv"))
	repliesBefore := transport.replyCount()

	// Same claims: the loop guard keeps the connection, the only reply is the refresh one.
	sendRateLimitCommand(client, &protocol.Command{
		Id:      2,
		Refresh: &protocol.RefreshRequest{Token: connTokenOf(t, mergeClaims(subs, map[string]any{"sid": "s-1"}))},
	})
	reply := transport.waitReply(t, 2)
	require.Nil(t, reply.Error)
	require.False(t, transport.disconnected())
	require.True(t, client.IsSubscribed("chat:srv"))
	require.Equal(t, repliesBefore+1, transport.replyCount())

	// Other claims: the connection is disconnected to resubscribe with the values of the new token.
	sendRateLimitCommand(client, &protocol.Command{
		Id:      3,
		Refresh: &protocol.RefreshRequest{Token: connTokenOf(t, mergeClaims(subs, map[string]any{"sid": "s-2"}))},
	})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectInsufficientState.Code, disconnect.Code)
}

// TestServerTagsFilterTemplateSubRefresh checks a subscription refresh with a new explicit filter keeps the template,
// and a refresh without a filter keeps the current filter.
func TestServerTagsFilterTemplateSubRefresh(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	subscribeClient(t, client, transport, 2, "chat:room", getSubscribeTokenHSWithServerTagsFilter("12", "chat:room", testServerTagsFilter()))

	newFilter := &protocol.FilterNode{Key: "type", Cmp: "eq", Val: "b"}
	sendRateLimitCommand(client, &protocol.Command{
		Id: 3,
		SubRefresh: &protocol.SubRefreshRequest{
			Channel: "chat:room",
			Token:   getSubscribeTokenHSWithServerTagsFilter("12", "chat:room", newFilter),
		},
	})
	require.Nil(t, transport.waitReply(t, 3).Error)

	// The template still drops the own session, and the new explicit filter applies.
	publishTagged(t, env.node, "chat:room", `{"n":1}`, map[string]string{"type": "b", "session": "s-1"})
	publishTagged(t, env.node, "chat:room", `{"n":2}`, map[string]string{"type": "b", "session": "s-2"})
	publishTagged(t, env.node, "chat:room", `{"n":3}`, map[string]string{"type": "a", "session": "s-2"})
	waitPushed(t, transport, `{"n":2}`)
	require.Equal(t, []string{`{"n":2}`}, transport.pushedData())

	// A refresh without a filter changes nothing.
	sendRateLimitCommand(client, &protocol.Command{
		Id: 4,
		SubRefresh: &protocol.SubRefreshRequest{
			Channel: "chat:room",
			Token:   getSubscribeTokenHS("12", "chat:room", 0),
		},
	})
	require.Nil(t, transport.waitReply(t, 4).Error)
	publishTagged(t, env.node, "chat:room", `{"n":4}`, map[string]string{"type": "b", "session": "s-1"})
	publishTagged(t, env.node, "chat:room", `{"n":5}`, map[string]string{"type": "b", "session": "s-3"})
	waitPushed(t, transport, `{"n":5}`)
	require.Equal(t, []string{`{"n":2}`, `{"n":5}`}, transport.pushedData())
}

// TestServerTagsFilterTemplateRebindResubscribes checks a rebind to a token with other claims unsubscribes the
// channels with a template, and the server-side subscriptions of the token come back with filters rendered with the
// new claims.
func TestServerTagsFilterTemplateRebindResubscribes(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	state.channels["lenient:room"] = centrifuge.ChannelContext{Source: subsource.ClientAllowed}
	state.channels["chat:room"] = centrifuge.ChannelContext{Source: subsource.ClientAllowed}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"sid":  "s-1",
			"subs": map[string]any{"lenient:srv": map[string]any{}},
		}),
	}, nil)
	require.NoError(t, err)

	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["lenient:room"].Code)
	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["chat:room"].Code)
	require.Contains(t, state.subscribed, "lenient:srv")
	require.True(t, proto.Equal(sessionFilter("s-1"), state.subscribed["lenient:srv"].ServerTagsFilter), "got %v", state.subscribed["lenient:srv"].ServerTagsFilter)
}

// TestServerTagsFilterTemplateRebindResubscribesUniConnect checks a uni-connect channel with a template, which an
// anonymous connection holds, is unsubscribed by a rebind with other values and subscribed again with the filter of the
// new values, since the connection keeps it.
func TestServerTagsFilterTemplateRebindResubscribesUniConnect(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	storage, release := client.AcquireStorage()
	storage[serverTagsFilterServerSideKey] = map[string]centrifuge.SubscribeOptions{
		"lenient:uni": {Source: subsource.UniConnect},
	}
	release(storage)
	state.channels["lenient:uni"] = centrifuge.ChannelContext{Source: subsource.UniConnect}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"sid": "s-1"}),
	}, nil)
	require.NoError(t, err)

	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["lenient:uni"].Code)
	require.Contains(t, state.subscribed, "lenient:uni")
	require.Equal(t, uint8(subsource.UniConnect), state.subscribed["lenient:uni"].Source)
	require.True(t, proto.Equal(sessionFilter("s-1"), state.subscribed["lenient:uni"].ServerTagsFilter), "got %v", state.subscribed["lenient:uni"].ServerTagsFilter)
}

// TestServerTagsFilterTemplateRebindKeepsConnectProxy checks a connect proxy channel with a template, which an anonymous
// connection holds in a skip namespace, is subscribed again by a rebind to a user with claims. The connection keeps the
// channel and the server does not subscribe it again on its own, so the rebind must restore it with the new filter.
func TestServerTagsFilterTemplateRebindKeepsConnectProxy(t *testing.T) {
	cfg := templateTestConfig()
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	cfg.RPC.Registry.AuthClassClaim = rebindTestClaim
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	require.NoError(t, h.setupRateLimit(h.cfgContainer.ConfigRef()))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	client, state := newRebindTestClient("", "anonymous")
	storage, release := client.AcquireStorage()
	storage[serverTagsFilterServerSideKey] = map[string]centrifuge.SubscribeOptions{
		"lenient:cp": {Source: subsource.ConnectProxy},
	}
	release(storage)
	state.channels["lenient:cp"] = centrifuge.ChannelContext{Source: subsource.ConnectProxy}

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{"sid": "s-1"}),
	}, nil)
	require.NoError(t, err)

	require.Equal(t, uint32(centrifuge.UnsubscribeCodeInsufficient), state.unsubscribed["lenient:cp"].Code)
	require.Contains(t, state.subscribed, "lenient:cp")
	require.Equal(t, uint8(subsource.ConnectProxy), state.subscribed["lenient:cp"].Source)
	require.True(t, proto.Equal(sessionFilter("s-1"), state.subscribed["lenient:cp"].ServerTagsFilter), "got %v", state.subscribed["lenient:cp"].ServerTagsFilter)
}

// TestServerTagsFilterTemplateServerSideSubscriptions checks only the uni-connect and connect proxy channels with a
// template are kept for a rebind. They keep their explicit filter, and not the template, since a rebind renders the
// template with its own values.
func TestServerTagsFilterTemplateServerSideSubscriptions(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	subs := map[string]centrifuge.SubscribeOptions{
		"lenient:uni": {Source: subsource.UniConnect},
		"lenient:cp":  {Source: subsource.ConnectProxy, ServerTagsFilter: sessionFilter("x")},
		"plain:uni":   {Source: subsource.UniConnect},
		"lenient:srv": {Source: subsource.ConnectionToken},
	}
	serverSide := env.h.serverSideSubscriptions(subs)
	require.Len(t, serverSide, 2)
	require.Equal(t, uint8(subsource.UniConnect), serverSide["lenient:uni"].Source)
	require.Equal(t, uint8(subsource.ConnectProxy), serverSide["lenient:cp"].Source)
	require.True(t, proto.Equal(sessionFilter("x"), serverSide["lenient:cp"].ServerTagsFilter))
	require.Nil(t, env.h.serverSideSubscriptions(map[string]centrifuge.SubscribeOptions{"lenient:srv": {Source: subsource.ConnectionToken}}))
}

// TestServerTagsFilterTemplateOffByDefault checks a config without templates keeps nothing: no storage key, no claims
// on the connection token, and no allocations in the helpers.
func TestServerTagsFilterTemplateOffByDefault(t *testing.T) {
	env := newTemplateEnv(t, config.DefaultConfig())
	client, _ := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1", "subs": map[string]any{"room": map[string]any{}}}))
	require.True(t, client.IsSubscribed("room"))
	storage, release := client.AcquireStorage()
	_, ok := storage[serverTagsFilterValuesKey]
	release(storage)
	require.False(t, ok)

	subs := map[string]centrifuge.SubscribeOptions{"room": {}}
	storageMap := map[string]any{}
	require.NoError(t, env.h.connectFilterTemplates("c1", nil, jwtverify.ConnectToken{}, false, subs, storageMap))
	connectAllocs := testing.AllocsPerRun(1000, func() {
		_ = env.h.connectFilterTemplates("c1", nil, jwtverify.ConnectToken{}, false, subs, storageMap)
	})
	require.Zero(t, connectAllocs)
	require.NotContains(t, storageMap, serverTagsFilterValuesKey)

	mock := newRegistryTestClient("c1", "12", nil, nil)
	reply := centrifuge.SubscribeReply{}
	subscribeAllocs := testing.AllocsPerRun(1000, func() {
		env.h.answerSubscribe(mock, "chat:room", reply, nil, noopSubscribeCallback)
	})
	require.Zero(t, subscribeAllocs)
	var answered centrifuge.SubscribeReply
	env.h.answerSubscribe(mock, "chat:room", reply, nil, func(r centrifuge.SubscribeReply, _ error) { answered = r })
	require.Nil(t, answered.Options.ServerTagsFilter)

	explicit := testServerTagsFilter()
	_, refreshErr := env.h.refreshFilterTemplate(mock, "chat:room", centrifuge.SubRefreshReply{ServerTagsFilter: explicit}, nil)
	require.NoError(t, refreshErr)
	refreshAllocs := testing.AllocsPerRun(1000, func() {
		_, _ = env.h.refreshFilterTemplate(mock, "chat:room", centrifuge.SubRefreshReply{ServerTagsFilter: explicit}, nil)
	})
	require.Zero(t, refreshAllocs)
}

// TestServerTagsFilterTemplateConfigReload checks a reload which adds a template: a connection verified before it has
// no claims, so its claim variables are missing and the policy applies, and a new connection is filtered.
func TestServerTagsFilterTemplateConfigReload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}}}
	env := newTemplateEnv(t, cfg)
	oldClient, oldTransport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	require.NotNil(t, oldClient)

	require.NoError(t, env.cfgContainer.Reload(templateTestConfig()))

	sendRateLimitCommand(oldClient, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "chat:room"}})
	reply := oldTransport.waitReply(t, 2)
	require.NotNil(t, reply.Error)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, reply.Error.Code)

	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	subscribeClient(t, client, transport, 2, "chat:room", "")
	publishTagged(t, env.node, "chat:room", `{"n":1}`, map[string]string{"session": "s-1"})
	publishTagged(t, env.node, "chat:room", `{"n":2}`, map[string]string{"session": "s-2"})
	waitPushed(t, transport, `{"n":2}`)
	require.Equal(t, []string{`{"n":2}`}, transport.pushedData())
}

// TestServerTagsFilterTemplateConnectProxyReloadAddsTemplate checks a reload which runs during the connect proxy call and
// adds a template to the namespace of a subscription the proxy returns: the subscription gets the template, because the
// templates are applied with the config read after the call, not with the config of the connect's start.
func TestServerTagsFilterTemplateConnectProxyReloadAddsTemplate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{{Name: "chat", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}}}
	env := newTemplateEnv(t, cfg)

	reloaded := config.DefaultConfig()
	reloaded.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "chat",
			ChannelOptions: configtypes.ChannelOptions{
				SubscribeForClient: true,
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "owner", "cmp": "eq", "val": "{{user}}"},
				},
			},
		},
	}
	connectProxyHandler := func(context.Context, centrifuge.ConnectEvent) (centrifuge.ConnectReply, proxy.ConnectExtra, error) {
		require.NoError(t, env.cfgContainer.Reload(reloaded))
		return centrifuge.ConnectReply{
			Credentials:   &centrifuge.Credentials{UserID: "42"},
			Subscriptions: map[string]centrifuge.SubscribeOptions{"chat:srv": {}},
		}, proxy.ConnectExtra{}, nil
	}

	reply, err := env.h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{ClientID: "c1"}, connectProxyHandler, true)
	require.NoError(t, err)
	want := &protocol.FilterNode{Key: "owner", Cmp: "eq", Val: "42"}
	require.True(t, proto.Equal(want, reply.Subscriptions["chat:srv"].ServerTagsFilter), "got %v", reply.Subscriptions["chat:srv"].ServerTagsFilter)
}

// TestServerTagsFilterTemplateUnsubscribeForgetsChannels checks that the record of templated channels keeps only the
// channels the connection still subscribes: each channel is recorded by its subscribe and removed by its unsubscribe, so
// the record is empty after all of them, and a long-lived connection does not grow it.
func TestServerTagsFilterTemplateUnsubscribeForgetsChannels(t *testing.T) {
	env := newTemplateEnv(t, templateTestConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))
	const channelCount int = 50
	for i := 0; i < channelCount; i++ {
		channel := fmt.Sprintf("chat:room-%d", i)
		subscribeID := uint32(2 + i)
		unsubscribeID := uint32(2 + channelCount + i)
		subscribeClient(t, client, transport, subscribeID, channel, "")
		require.Contains(t, templatedChannelsOf(client), channel)
		sendRateLimitCommand(client, &protocol.Command{Id: unsubscribeID, Unsubscribe: &protocol.UnsubscribeRequest{Channel: channel}})
		reply := transport.waitReply(t, unsubscribeID)
		require.Nil(t, reply.Error)
	}
	require.Eventually(t, func() bool {
		return len(templatedChannelsOf(client)) == 0
	}, 5*time.Second, time.Millisecond)
}
