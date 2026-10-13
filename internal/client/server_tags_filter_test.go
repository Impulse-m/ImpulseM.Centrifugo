package client

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

func testServerTagsFilter() *protocol.FilterNode {
	return &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			{Key: "type", Cmp: "eq", Val: "a"},
			{Key: "tier", Cmp: "in", Vals: []string{"x", "y"}},
		},
	}
}

func getSubscribeTokenHSWithServerTagsFilter(user string, channel string, serverTagsFilter *protocol.FilterNode) string {
	builder := getTokenBuilder(nil, "secret")
	claims := &jwtverify.SubscribeTokenClaims{
		SubscribeOptions: jwtverify.SubscribeOptions{
			ServerTagsFilter: serverTagsFilter,
		},
		Channel: channel,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: user,
		},
	}
	token, err := builder.Build(claims)
	if err != nil {
		panic(err)
	}
	return token.String()
}

func TestClientSubscribeWithTokenServerTagsFilter(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	transport := tools.NewTestTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "12",
			},
		}, nil
	})

	connectCommand := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	encoder := protocol.NewJSONCommandEncoder()
	data, err := encoder.Encode(connectCommand)
	require.NoError(t, err)
	ok := centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20)
	require.True(t, ok)

	reply, _, err := h.OnSubscribe(client, centrifuge.SubscribeEvent{
		Channel: "$test1",
		Token:   getSubscribeTokenHSWithServerTagsFilter("12", "$test1", testServerTagsFilter()),
	}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, testServerTagsFilter(), reply.Options.ServerTagsFilter)
}

func TestClientOnSubscribe_ServerTagsFilterFromProxy(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfg.Channel.Proxy.Subscribe.Endpoint = "http://localhost:8080"
	cfg.Channel.WithoutNamespace.SubscribeProxyEnabled = true
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	h := NewHandler(node, cfgContainer, nil, nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	proxyFunc := func(c proxy.Client, e centrifuge.SubscribeEvent, chOpts configtypes.ChannelOptions, pcd proxy.PerCallData) (centrifuge.SubscribeReply, proxy.SubscribeExtra, error) {
		return centrifuge.SubscribeReply{
			Options: centrifuge.SubscribeOptions{
				ServerTagsFilter: testServerTagsFilter(),
			},
			ClientSideRefresh: !chOpts.SubRefreshProxyEnabled,
		}, proxy.SubscribeExtra{}, nil
	}

	reply, _, err := h.OnSubscribe(&tools.TestClientMock{
		UserIDFunc: func() string {
			return "42"
		},
	}, centrifuge.SubscribeEvent{
		Channel: "user",
	}, proxyFunc, nil)
	require.NoError(t, err)
	require.Equal(t, testServerTagsFilter(), reply.Options.ServerTagsFilter)
}

func TestClientSideSubRefresh_ServerTagsFilter(t *testing.T) {
	node := tools.NodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)

	transport := tools.NewTestTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "12",
			},
		}, nil
	})

	connectCommand := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	encoder := protocol.NewJSONCommandEncoder()
	data, err := encoder.Encode(connectCommand)
	require.NoError(t, err)
	ok := centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20)
	require.True(t, ok)

	subRefreshReply, _, err := h.OnSubRefresh(client, nil, centrifuge.SubRefreshEvent{
		Channel: "$test1",
		Token:   getSubscribeTokenHSWithServerTagsFilter("12", "$test1", testServerTagsFilter()),
	})
	require.NoError(t, err)
	require.Equal(t, testServerTagsFilter(), subRefreshReply.ServerTagsFilter)

	subRefreshReply, _, err = h.OnSubRefresh(client, nil, centrifuge.SubRefreshEvent{
		Channel: "$test1",
		Token:   getSubscribeTokenHS("12", "$test1", 0),
	})
	require.NoError(t, err)
	require.Nil(t, subRefreshReply.ServerTagsFilter)
}

func TestTokenChannelsChangedServerTagsFilter(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())

	client, _ := connectWithConnToken(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub":  "12",
		"subs": map[string]any{"test1": map[string]any{"server_tags_filter": testServerTagsFilter()}},
	}))
	require.True(t, client.IsSubscribed("test1"))

	// Same filter content, different pointer: no change.
	require.False(t, tokenChannelsChanged(client, map[string]centrifuge.SubscribeOptions{
		"test1": {ServerTagsFilter: testServerTagsFilter()},
	}))

	// Different filter: change.
	otherFilter := testServerTagsFilter()
	otherFilter.Nodes[0].Val = "b"
	require.True(t, tokenChannelsChanged(client, map[string]centrifuge.SubscribeOptions{
		"test1": {ServerTagsFilter: otherFilter},
	}))

	// Filter removed: change.
	require.True(t, tokenChannelsChanged(client, map[string]centrifuge.SubscribeOptions{
		"test1": {},
	}))
}

// connectWithConnToken connects a client with the given connection token through the handler
// and returns it with its transport.
func connectWithConnToken(t *testing.T, node *centrifuge.Node, token string) (*centrifuge.Client, *recordingTransport) {
	t.Helper()
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Token: token}})
	reply := transport.waitReply(t, 1)
	require.Nil(t, reply.Error)
	return client, transport
}

// TestRefreshServerTagsFilterChange checks a connection refresh with the same server tags filter keeps
// the connection, and a refresh with a different filter disconnects it to apply the new subscriptions.
func TestRefreshServerTagsFilterChange(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())

	tokenWithFilter := func(filter *protocol.FilterNode) string {
		return getConnTokenWithClaims(t, map[string]any{
			"sub":  "12",
			"subs": map[string]any{"test1": map[string]any{"server_tags_filter": filter}},
		})
	}
	otherFilter := testServerTagsFilter()
	otherFilter.Nodes[0].Val = "b"

	client, transport := connectWithConnToken(t, node, tokenWithFilter(testServerTagsFilter()))
	require.True(t, client.IsSubscribed("test1"))
	repliesBefore := transport.replyCount()

	// Same filter content: the connection stays and nothing is resubscribed, so the only reply is the refresh one.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Refresh: &protocol.RefreshRequest{Token: tokenWithFilter(testServerTagsFilter())}})
	reply := transport.waitReply(t, 2)
	require.Nil(t, reply.Error)
	require.False(t, transport.disconnected())
	require.True(t, client.IsSubscribed("test1"))
	require.Equal(t, repliesBefore+1, transport.replyCount())

	// Different filter: the refresh leads to a disconnect with insufficient state.
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Refresh: &protocol.RefreshRequest{Token: tokenWithFilter(otherFilter)}})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectInsufficientState.Code, disconnect.Code)
}
