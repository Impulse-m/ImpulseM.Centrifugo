package client

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/api"
	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestUnsubscribeResubscribeCodeReachesClientAndAllowsSubscribe checks what the server owns for a server unsubscribe
// with a resubscribe code: the push carries the code and reason, the server no longer holds the subscription after
// the push, and a subscribe sent after it is accepted. The server cannot make the client subscribe again by itself:
// that is the SDK's reaction to a code of 2500 or more, and the SDKs' own tests cover it. The helpers of this package
// have no subscribe proxy stub, so the test does not check that the proxy runs again and no proxy fixture is built
// here. The default unsubscribe of an absent code is checked too.
func TestUnsubscribeResubscribeCodeReachesClientAndAllowsSubscribe(t *testing.T) {
	h, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{}, []configtypes.ChannelNamespace{
		{Name: "ns", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}},
	})
	executor := api.NewExecutor(node, h.cfgContainer, nil, api.ExecutorConfig{Protocol: "test"})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "ns:room"}})
	require.Nil(t, transport.waitReply(t, 2).Error)
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Subscribe: &protocol.SubscribeRequest{Channel: "ns:other"}})
	require.Nil(t, transport.waitReply(t, 3).Error)

	resp := executor.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{
		Channel:     "ns:room",
		Unsubscribe: &apiproto.Unsubscribe{Code: 2600, Reason: "resubscribe"},
	})
	require.Nil(t, resp.Error)
	push := transport.waitUnsubscribePush(t, "ns:room")
	require.Equal(t, uint32(2600), push.Code)
	require.Equal(t, "resubscribe", push.Reason)
	require.False(t, client.IsSubscribed("ns:room"))
	require.Equal(t, 0, node.Hub().NumSubscribers("ns:room"), "the server still holds the subscription after the push")

	// The subscribe the SDK sends after a 2600 unsubscribe must be accepted by the server.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "ns:room"}})
	require.Nil(t, transport.waitReply(t, 4).Error)
	require.True(t, client.IsSubscribed("ns:room"))
	require.Equal(t, 1, node.Hub().NumSubscribers("ns:room"))

	resp = executor.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{Channel: "ns:other"})
	require.Nil(t, resp.Error)
	push = transport.waitUnsubscribePush(t, "ns:other")
	require.Equal(t, uint32(2000), push.Code)
	require.False(t, client.IsSubscribed("ns:other"))
}
