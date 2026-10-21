package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/api"
	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// waitUnsubscribePush waits until the client is sent an unsubscribe push for the channel and returns it.
func (t *recordingTransport) waitUnsubscribePush(tb testing.TB, channel string) *protocol.Unsubscribe {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		for _, reply := range t.replies {
			if reply.Push != nil && reply.Push.Channel == channel && reply.Push.Unsubscribe != nil {
				t.mu.Unlock()
				return reply.Push.Unsubscribe
			}
		}
		t.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("unsubscribe push for channel %s not received", channel)
	return nil
}

// TestUnsubscribeResubscribeCodeReachesClient checks a server unsubscribe with a resubscribe code reaches the connected
// client with the code and reason, and the client subscribes again with the command an SDK sends for that code. The
// default unsubscribe of an absent code is checked too.
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

	// The SDK resubscribes on a resubscribe code, so the client subscribes again on its own.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "ns:room"}})
	require.Nil(t, transport.waitReply(t, 4).Error)
	require.True(t, client.IsSubscribed("ns:room"))

	resp = executor.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{Channel: "ns:other"})
	require.Nil(t, resp.Error)
	push = transport.waitUnsubscribePush(t, "ns:other")
	require.Equal(t, uint32(2000), push.Code)
	require.False(t, client.IsSubscribed("ns:other"))
}
