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

// TestUnsubscribeResubscribeCodeReachesClient checks a server unsubscribe with a resubscribe code reaches the connected
// client with the code and reason. The subscribe after it is the one the SDK sends for code 2600: the test sends it
// itself, so the resubscribe is simulated here and not triggered by the server. The SDK behaviour is covered by the
// SDKs' own tests. The default unsubscribe of an absent code is checked too.
func TestUnsubscribeResubscribeCodeReachesClient(t *testing.T) {
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

	// The simulated resubscribe: the subscribe the SDK sends after a 2600 unsubscribe. It shows the server accepts it.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "ns:room"}})
	require.Nil(t, transport.waitReply(t, 4).Error)
	require.True(t, client.IsSubscribed("ns:room"))

	resp = executor.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{Channel: "ns:other"})
	require.Nil(t, resp.Error)
	push = transport.waitUnsubscribePush(t, "ns:other")
	require.Equal(t, uint32(2000), push.Code)
	require.False(t, client.IsSubscribed("ns:other"))
}
