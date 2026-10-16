package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// historyTemplateConfig returns the session template config with two namespaces with history for clients: "hist"
// has the session template, "plainhist" has none and shows that history works without a template.
func historyTemplateConfig() config.Config {
	cfg := templateTestConfig()
	history := configtypes.ChannelOptions{
		SubscribeForClient: true,
		HistorySize:        10,
		HistoryTTL:         configtypes.Duration(time.Minute),
		HistoryForClient:   true,
	}
	templated := history
	templated.ServerTagsFilterTemplate = configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
	}
	cfg.Channel.Namespaces = append(cfg.Channel.Namespaces,
		configtypes.ChannelNamespace{Name: "hist", ChannelOptions: templated},
		configtypes.ChannelNamespace{Name: "plainhist", ChannelOptions: history},
	)
	return cfg
}

// requestHistory sends a history command for channel with the command id and returns the error of its reply.
func requestHistory(t *testing.T, client *centrifuge.Client, transport *recordingTransport, id uint32, channel string) *protocol.Error {
	t.Helper()
	sendRateLimitCommand(client, &protocol.Command{Id: id, History: &protocol.HistoryRequest{Channel: channel}})
	return transport.waitReply(t, id).Error
}

// TestServerTagsFilterTemplateRefusesHistory checks history is refused on a channel of a namespace with a template
// whether the client is subscribed or not, and also for an insecure server, while history without a template works.
func TestServerTagsFilterTemplateRefusesHistory(t *testing.T) {
	env := newTemplateEnv(t, historyTemplateConfig())
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))

	require.Nil(t, requestHistory(t, client, transport, 2, "plainhist:room"))

	// Not subscribed: the history would read the publications of every session.
	err := requestHistory(t, client, transport, 3, "hist:room")
	require.NotNil(t, err)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, err.Code)

	subscribeClient(t, client, transport, 4, "hist:room", "")
	err = requestHistory(t, client, transport, 5, "hist:room")
	require.NotNil(t, err)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, err.Code)

	// After the unsubscribe the client is not subscribed again, and the history is still refused.
	sendRateLimitCommand(client, &protocol.Command{Id: 6, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "hist:room"}})
	require.Nil(t, transport.waitReply(t, 6).Error)
	err = requestHistory(t, client, transport, 7, "hist:room")
	require.NotNil(t, err)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, err.Code)
}

// TestServerTagsFilterTemplateRefusesHistoryInsecure checks the insecure server, which allows history to every
// connection, still refuses history on a channel with a template.
func TestServerTagsFilterTemplateRefusesHistoryInsecure(t *testing.T) {
	cfg := historyTemplateConfig()
	cfg.Client.Insecure = true
	env := newTemplateEnv(t, cfg)
	client, transport := connectWithConnToken(t, env.node, connTokenOf(t, map[string]any{"sid": "s-1"}))

	require.Nil(t, requestHistory(t, client, transport, 2, "plainhist:room"))
	err := requestHistory(t, client, transport, 3, "hist:room")
	require.NotNil(t, err)
	require.Equal(t, centrifuge.ErrorPermissionDenied.Code, err.Code)
}
