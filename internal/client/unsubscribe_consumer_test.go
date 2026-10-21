package client

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/api"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestConsumingUnsubscribeInvalidCodeAppliesDefault checks that a consumer unsubscribe record with a code outside the
// custom ranges is dropped from the record and applied with the default code 2000, and that the invalid code is logged.
// The record goes through the same dispatch as a consumer of the unsubscribe method.
func TestConsumingUnsubscribeInvalidCodeAppliesDefault(t *testing.T) {
	var logBuf syncBuffer
	savedLogger := log.Logger
	log.Logger = zerolog.New(&logBuf)
	defer func() { log.Logger = savedLogger }()

	h, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{}, []configtypes.ChannelNamespace{
		{Name: "ns", ChannelOptions: configtypes.ChannelOptions{SubscribeForClient: true}},
	})
	executor := api.NewExecutor(node, h.cfgContainer, nil, api.ExecutorConfig{Protocol: "test"})
	dispatcher := api.NewDispatcher(api.NewConsumingHandler(node, executor, api.ConsumingHandlerConfig{}))
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "ns:room"}})
	require.Nil(t, transport.waitReply(t, 2).Error)

	record := []byte(`{"channel":"ns:room","unsubscribe":{"code":2050}}`)
	err := dispatcher.DispatchCommand(context.Background(), "unsubscribe", record)
	require.NoError(t, err)

	push := transport.waitUnsubscribePush(t, "ns:room")
	require.Equal(t, uint32(2000), push.Code)
	require.False(t, client.IsSubscribed("ns:room"))
	require.Equal(t, 0, node.Hub().NumSubscribers("ns:room"))
	require.Contains(t, logBuf.String(), "ignoring invalid unsubscribe")
}
