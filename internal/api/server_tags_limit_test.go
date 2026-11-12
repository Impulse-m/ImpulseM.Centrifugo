package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// serverTagsTestExecutor returns a server API executor over a memory node with four namespaces: st allows server tags
// with history, plain has history but does not allow server tags, caps allows them with history and limits the tags of
// a publication to 3 entries and 20 bytes, and no namespace is used for channels without namespace.
func serverTagsTestExecutor(t *testing.T) *Executor {
	t.Helper()
	_, api := serverTagsTestSetup(t)
	return api
}

// serverTagsTestSetup returns the memory node and the server API executor of serverTagsTestExecutor.
func serverTagsTestSetup(t *testing.T) (*centrifuge.Node, *Executor) {
	t.Helper()
	node := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Engine.Type = "memory"
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "st",
			ChannelOptions: configtypes.ChannelOptions{
				AllowServerTags: true,
				HistorySize:     10,
				HistoryTTL:      configtypes.Duration(time.Minute),
			},
		},
		{
			Name: "plain",
			ChannelOptions: configtypes.ChannelOptions{
				HistorySize: 10,
				HistoryTTL:  configtypes.Duration(time.Minute),
			},
		},
		{
			Name: "caps",
			ChannelOptions: configtypes.ChannelOptions{
				AllowServerTags:     true,
				HistorySize:         10,
				HistoryTTL:          configtypes.Duration(time.Minute),
				PublishTagsMaxCount: 3,
				PublishTagsMaxBytes: 20,
			},
		},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	return node, NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
}

// TestPublishServerTagsNotAllowedInNamespace checks a publish with server tags is refused with 108 in a namespace which
// does not allow them, and that a publish without server tags is the same as before the option.
func TestPublishServerTagsNotAllowedInNamespace(t *testing.T) {
	api := serverTagsTestExecutor(t)
	resp := api.Publish(context.Background(), &PublishRequest{
		Channel:    "plain:room",
		Data:       []byte("{}"),
		ServerTags: map[string]string{"tenant": "t-17"},
	})
	require.Equal(t, ErrorNotAvailable, resp.Error)

	resp = api.Publish(context.Background(), &PublishRequest{Channel: "plain:room", Data: []byte("{}")})
	require.Nil(t, resp.Error)
}

// TestPublishServerTagsKeyRules checks the reserved-key and collision rules: an empty key, a key with the reserved
// prefix and a key which is also in tags are refused with 107, and a valid set is accepted.
func TestPublishServerTagsKeyRules(t *testing.T) {
	api := serverTagsTestExecutor(t)
	for _, tc := range []struct {
		name       string
		tags       map[string]string
		serverTags map[string]string
		want       *Error
	}{
		{"empty key", map[string]string{"kind": "msg"}, map[string]string{"": "x"}, ErrorBadRequest},
		{"reserved prefix", nil, map[string]string{"__centrifugo_epoch": "x"}, ErrorBadRequest},
		{"key also in tags", map[string]string{"tenant": "public"}, map[string]string{"tenant": "t-17"}, ErrorBadRequest},
		{"valid", map[string]string{"kind": "msg"}, map[string]string{"tenant": "t-17"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := api.Publish(context.Background(), &PublishRequest{
				Channel:    "st:room",
				Data:       []byte("{}"),
				Tags:       tc.tags,
				ServerTags: tc.serverTags,
			})
			require.Equal(t, tc.want, resp.Error)
		})
	}
}

// TestPublishServerTagsSkipHistoryAccepted checks skip_history is allowed with server tags: the publish goes out, and
// it is not stored, so it is never recovered.
func TestPublishServerTagsSkipHistoryAccepted(t *testing.T) {
	api := serverTagsTestExecutor(t)
	resp := api.Publish(context.Background(), &PublishRequest{
		Channel:     "st:room",
		Data:        []byte("{}"),
		SkipHistory: true,
		ServerTags:  map[string]string{"tenant": "t-17"},
	})
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Result)

	history := api.History(context.Background(), &HistoryRequest{Channel: "st:room", Limit: -1})
	require.Nil(t, history.Error)
	require.Empty(t, history.Result.Publications)
}

// TestPublishServerTagsCaps checks the caps count tags and server tags together: a publish at the limit passes, and one
// over it is refused with 1000. The count is 3 entries, the bytes are 20 bytes of keys and values.
func TestPublishServerTagsCaps(t *testing.T) {
	api := serverTagsTestExecutor(t)
	ctx := context.Background()

	resp := api.Publish(ctx, &PublishRequest{
		Channel:    "caps:room",
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1", "b": "2"},
		ServerTags: map[string]string{"c": "3"},
	})
	require.Nil(t, resp.Error, "3 entries is at the count limit")

	resp = api.Publish(ctx, &PublishRequest{
		Channel:    "caps:room",
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1", "b": "2"},
		ServerTags: map[string]string{"c": "3", "d": "4"},
	})
	require.Equal(t, ErrorPublishTagsLimit, resp.Error, "4 entries is over the count limit")

	// "a"+"1" and "b"+17 bytes are 20 bytes: at the bytes limit.
	resp = api.Publish(ctx, &PublishRequest{
		Channel:    "caps:room",
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1"},
		ServerTags: map[string]string{"b": strings.Repeat("x", 17)},
	})
	require.Nil(t, resp.Error, "20 bytes is at the bytes limit")

	// One more byte in the value is 21 bytes: over the bytes limit.
	resp = api.Publish(ctx, &PublishRequest{
		Channel:    "caps:room",
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1"},
		ServerTags: map[string]string{"b": strings.Repeat("x", 18)},
	})
	require.Equal(t, ErrorPublishTagsLimit, resp.Error, "21 bytes is over the bytes limit")
}

// TestRefusedPublishStoresNothing checks that a publish refused by the gate, by the key rules or by a cap stores nothing
// in the history of its channel.
func TestRefusedPublishStoresNothing(t *testing.T) {
	api := serverTagsTestExecutor(t)
	ctx := context.Background()

	resp := api.Publish(ctx, &PublishRequest{
		Channel:    "plain:room",
		Data:       []byte("{}"),
		ServerTags: map[string]string{"tenant": "t-17"},
	})
	require.Equal(t, ErrorNotAvailable, resp.Error)

	resp = api.Publish(ctx, &PublishRequest{
		Channel:    "st:room",
		Data:       []byte("{}"),
		ServerTags: map[string]string{"__x": "1"},
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	resp = api.Publish(ctx, &PublishRequest{
		Channel:    "caps:room",
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"},
		ServerTags: map[string]string{"e": "5"},
	})
	require.Equal(t, ErrorPublishTagsLimit, resp.Error)

	for _, channel := range []string{"plain:room", "st:room", "caps:room"} {
		history := api.History(ctx, &HistoryRequest{Channel: channel, Limit: -1})
		require.Nil(t, history.Error)
		require.Empty(t, history.Result.Publications, channel)
	}
}

// TestHistoryDoesNotReturnServerTags checks the server API history returns the public tags of a publication only.
func TestHistoryDoesNotReturnServerTags(t *testing.T) {
	api := serverTagsTestExecutor(t)
	ctx := context.Background()
	resp := api.Publish(ctx, &PublishRequest{
		Channel:    "st:hist",
		Data:       []byte("{}"),
		Tags:       map[string]string{"kind": "msg"},
		ServerTags: map[string]string{"tenant": "t-17"},
	})
	require.Nil(t, resp.Error)

	history := api.History(ctx, &HistoryRequest{Channel: "st:hist", Limit: -1})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1)
	require.Equal(t, map[string]string{"kind": "msg"}, history.Result.Publications[0].Tags)
}

// TestBroadcastServerTagsCapPerChannel checks a broadcast over the cap of one channel refuses that channel's response
// only: the other channel of the same request is published.
func TestBroadcastServerTagsCapPerChannel(t *testing.T) {
	api := serverTagsTestExecutor(t)
	resp := api.Broadcast(context.Background(), &BroadcastRequest{
		Channels:   []string{"caps:a", "st:b"},
		Data:       []byte("{}"),
		Tags:       map[string]string{"a": "1"},
		ServerTags: map[string]string{"b": "2", "c": "3", "d": "4"},
	})
	require.Nil(t, resp.Error)
	require.Len(t, resp.Result.Responses, 2)
	require.Equal(t, ErrorPublishTagsLimit, resp.Result.Responses[0].Error)
	require.Nil(t, resp.Result.Responses[1].Error)
}

// TestBroadcastServerTagsGatePerChannel checks the gate of each channel's namespace applies to that channel's response.
func TestBroadcastServerTagsGatePerChannel(t *testing.T) {
	api := serverTagsTestExecutor(t)
	resp := api.Broadcast(context.Background(), &BroadcastRequest{
		Channels:   []string{"st:b", "plain:c"},
		Data:       []byte("{}"),
		ServerTags: map[string]string{"tenant": "t-17"},
	})
	require.Nil(t, resp.Error)
	require.Len(t, resp.Result.Responses, 2)
	require.Nil(t, resp.Result.Responses[0].Error)
	require.Equal(t, ErrorNotAvailable, resp.Result.Responses[1].Error)
}

// TestBroadcastServerTagsKeysRefuseRequest checks a reserved key in a broadcast refuses the whole call with 107, since
// the keys are the same for every channel.
func TestBroadcastServerTagsKeysRefuseRequest(t *testing.T) {
	api := serverTagsTestExecutor(t)
	resp := api.Broadcast(context.Background(), &BroadcastRequest{
		Channels:   []string{"st:b", "st:c"},
		Data:       []byte("{}"),
		ServerTags: map[string]string{"__x": "1"},
	})
	require.Equal(t, ErrorBadRequest, resp.Error)
	require.Nil(t, resp.Result)
}

// TestBroadcastSharedServerTagsMapNotMutated checks one request map shared by the channels of a broadcast is read and
// not written. It runs the channels concurrently, so the race detector covers the shared map.
func TestBroadcastSharedServerTagsMapNotMutated(t *testing.T) {
	api := serverTagsTestExecutor(t)
	serverTags := map[string]string{"tenant": "t-17", "member": "u-42"}
	channels := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		channels = append(channels, fmt.Sprintf("st:room%d", i))
	}
	resp := api.Broadcast(context.Background(), &BroadcastRequest{
		Channels:   channels,
		Data:       []byte("{}"),
		ServerTags: serverTags,
	})
	require.Nil(t, resp.Error)
	require.Len(t, resp.Result.Responses, len(channels))
	for _, r := range resp.Result.Responses {
		require.Nil(t, r.Error)
	}
	require.Equal(t, map[string]string{"tenant": "t-17", "member": "u-42"}, serverTags)
}

// TestCheckServerTagsAllowed checks the gate on the channel options: a request without server tags is never refused, and
// a request with them needs allow_server_tags on a stream channel.
func TestCheckServerTagsAllowed(t *testing.T) {
	serverTags := map[string]string{"tenant": "t-17"}
	for _, tc := range []struct {
		name       string
		opts       configtypes.ChannelOptions
		serverTags map[string]string
		want       *Error
	}{
		{"no server tags", configtypes.ChannelOptions{}, nil, nil},
		{"gate off", configtypes.ChannelOptions{}, serverTags, ErrorNotAvailable},
		{"stream", configtypes.ChannelOptions{AllowServerTags: true}, serverTags, nil},
		{"stream by name", configtypes.ChannelOptions{AllowServerTags: true, SubscriptionType: "stream"}, serverTags, nil},
		{"map", configtypes.ChannelOptions{AllowServerTags: true, SubscriptionType: "map"}, serverTags, ErrorNotAvailable},
		{"shared poll", configtypes.ChannelOptions{AllowServerTags: true, SubscriptionType: "shared_poll"}, serverTags, ErrorNotAvailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, checkServerTagsAllowed(&tc.opts, tc.serverTags))
		})
	}
}

// TestMeasureTags checks the count and the bytes of the tags of a request count both maps.
func TestMeasureTags(t *testing.T) {
	size := measureTags(map[string]string{"kind": "msg"}, map[string]string{"tenant": "t-17"})
	require.Equal(t, tagsSize{count: 2, bytes: len("kind") + len("msg") + len("tenant") + len("t-17")}, size)
	require.Equal(t, tagsSize{}, measureTags(nil, nil))
}
