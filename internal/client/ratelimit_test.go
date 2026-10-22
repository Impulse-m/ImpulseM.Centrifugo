package client

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// recordingTransport records replies and disconnects of a test client connection.
type recordingTransport struct {
	*tools.TestTransport
	mu         sync.Mutex
	replies    []*protocol.Reply
	disconnect *centrifuge.Disconnect
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{TestTransport: tools.NewTestTransport()}
}

func (t *recordingTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

func (t *recordingTransport) WriteMany(messages ...[]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, message := range messages {
		decoder := protocol.NewJSONReplyDecoder(message)
		for {
			reply, err := decoder.Decode()
			if err != nil {
				break
			}
			t.replies = append(t.replies, reply)
		}
	}
	return nil
}

func (t *recordingTransport) Close(disconnect centrifuge.Disconnect) error {
	t.mu.Lock()
	t.disconnect = &disconnect
	t.mu.Unlock()
	return t.TestTransport.Close(disconnect)
}

// waitReply waits until a reply with the given command id is received.
func (t *recordingTransport) waitReply(tb testing.TB, id uint32) *protocol.Reply {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		for _, reply := range t.replies {
			if reply.Id == id {
				t.mu.Unlock()
				return reply
			}
		}
		t.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("reply for command %d not received", id)
	return nil
}

// waitDisconnect waits until the connection is closed and returns the disconnect code.
func (t *recordingTransport) waitDisconnect(tb testing.TB) centrifuge.Disconnect {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		disconnect := t.disconnect
		t.mu.Unlock()
		if disconnect != nil {
			return *disconnect
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("connection was not disconnected")
	return centrifuge.Disconnect{}
}

// replyCount returns the number of received replies.
func (t *recordingTransport) replyCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.replies)
}

func (t *recordingTransport) disconnected() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.disconnect != nil
}

func sendRateLimitCommand(client *centrifuge.Client, cmd *protocol.Command) {
	data, err := protocol.NewJSONCommandEncoder().Encode(cmd)
	if err != nil {
		panic(err)
	}
	centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20)
}

// newRateLimitHandler creates a handler with a real Core node and the given rate limit config. Each
// namespace is a stream namespace.
func newRateLimitHandler(t *testing.T, rl configtypes.RateLimit, namespaces ...string) *centrifuge.Node {
	t.Helper()
	channelNamespaces := make([]configtypes.ChannelNamespace, 0, len(namespaces))
	for _, name := range namespaces {
		channelNamespaces = append(channelNamespaces, configtypes.ChannelNamespace{Name: name})
	}
	_, node := newRateLimitHandlerWithNamespaces(t, rl, channelNamespaces)
	return node
}

// newRateLimitHandlerWithNamespaces is newRateLimitHandler with namespaces of any subscription type. It
// returns the handler too.
func newRateLimitHandlerWithNamespaces(
	t *testing.T,
	rl configtypes.RateLimit,
	namespaces []configtypes.ChannelNamespace,
) (*Handler, *centrifuge.Node) {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.RateLimit = rl
	// A shared poll namespace needs the HMAC key and the default shared poll proxy.
	cfg.SharedPoll.HMACSecretKey = "test-secret"
	cfg.Channel.Proxy.SharedPollRefresh.Endpoint = "http://localhost:9999"
	cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaces...)
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return h, node
}

// connectTestClient connects an anonymous client to node, which advertises flag in its connect request as an SDK
// does. It returns the client, its transport and the connect result.
func connectTestClient(t *testing.T, node *centrifuge.Node, flag int64) (*centrifuge.Client, *recordingTransport, *protocol.ConnectResult) {
	t.Helper()
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Flag: flag}})
	reply := transport.waitReply(t, 1)
	require.Nil(t, reply.Error)
	require.NotNil(t, reply.Connect)
	return client, transport, reply.Connect
}

func isRateLimited(reply *protocol.Reply) bool {
	return reply.Error != nil && reply.Error.Code == centrifuge.ErrorTooManyRequests.Code
}

func rateLimitBucketsOf(interval time.Duration, rate int) configtypes.RateLimitBuckets {
	return configtypes.RateLimitBuckets{
		Enabled: true,
		Buckets: configtypes.RateLimitBucketList{{Interval: configtypes.Duration(interval), Rate: rate}},
	}
}

func rateLimitChannelOf(interval time.Duration, rate int) configtypes.RateLimitChannelBuckets {
	buckets := rateLimitBucketsOf(interval, rate)
	return configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: buckets.Buckets}
}

func TestRateLimitSubscribeOverLimit(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Subscribe: rateLimitChannelOf(time.Minute, 2),
			},
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)
	before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("client_command", "subscribe"))

	for id := uint32(2); id <= 3; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Subscribe: &protocol.SubscribeRequest{Channel: "test:1"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "command %d must be allowed", id)
	}
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "test:1"}})
	reply := transport.waitReply(t, 4)
	require.NotNil(t, reply.Error)
	require.Equal(t, centrifuge.ErrorTooManyRequests.Code, reply.Error.Code)
	require.Equal(t, uint32(111), reply.Error.Code)

	after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("client_command", "subscribe"))
	require.Equal(t, float64(1), after-before)

	// Limits are per connection: another connection is not affected.
	other, otherTransport, _ := connectTestClient(t, node, 0)
	sendRateLimitCommand(other, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(otherTransport.waitReply(t, 2)))
}

func TestRateLimitUnsubscribeHistoryPresence(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Unsubscribe: rateLimitChannelOf(time.Minute, 1),
				History:     rateLimitChannelOf(time.Minute, 1),
				Presence:    rateLimitChannelOf(time.Minute, 1),
			},
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	require.True(t, isRateLimited(transport.waitReply(t, 3)))

	sendRateLimitCommand(client, &protocol.Command{Id: 4, History: &protocol.HistoryRequest{Channel: "test:1"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 5, History: &protocol.HistoryRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 4)))
	require.True(t, isRateLimited(transport.waitReply(t, 5)))

	sendRateLimitCommand(client, &protocol.Command{Id: 6, Presence: &protocol.PresenceRequest{Channel: "test:1"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 7, Presence: &protocol.PresenceRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 6)))
	require.True(t, isRateLimited(transport.waitReply(t, 7)))
}

func TestRateLimitTotalOrdering(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			Total:   rateLimitBucketsOf(time.Minute, 3),
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: rateLimitChannelOf(time.Minute, 1),
			},
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Publish: &protocol.PublishRequest{Channel: "test:1"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Publish: &protocol.PublishRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	require.True(t, isRateLimited(transport.waitReply(t, 3)))

	// The rejected publish must not have consumed total: two more commands fit.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, History: &protocol.HistoryRequest{Channel: "test:1"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 5, History: &protocol.HistoryRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 4)))
	require.False(t, isRateLimited(transport.waitReply(t, 5)))

	sendRateLimitCommand(client, &protocol.Command{Id: 6, Presence: &protocol.PresenceRequest{Channel: "test:1"}})
	require.True(t, isRateLimited(transport.waitReply(t, 6)))
}

func TestRateLimitRPCMethodOverride(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				RPC: configtypes.RateLimitRPCBuckets{
					Enabled: true,
					Buckets: rateLimitBucketsOf(time.Minute, 10).Buckets,
					MethodOverrides: configtypes.RateLimitMethodOverrides{
						{Method: "slow", Enabled: true, Buckets: rateLimitBucketsOf(time.Minute, 1).Buckets},
					},
				},
			},
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Rpc: &protocol.RPCRequest{Method: "slow"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Rpc: &protocol.RPCRequest{Method: "slow"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	require.True(t, isRateLimited(transport.waitReply(t, 3)))

	sendRateLimitCommand(client, &protocol.Command{Id: 4, Rpc: &protocol.RPCRequest{Method: "fast"}})
	require.False(t, isRateLimited(transport.waitReply(t, 4)))
}

func TestRateLimitNamespaceOverride(t *testing.T) {
	publish := rateLimitChannelOf(time.Minute, 1)
	publish.NamespaceOverrides = configtypes.RateLimitNamespaceOverrides{
		{NamespaceName: "chat", Enabled: true, Buckets: rateLimitBucketsOf(time.Minute, 3).Buckets},
	}
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: publish,
			},
		},
	}, "chat")
	client, transport, _ := connectTestClient(t, node, 0)

	for id := uint32(2); id <= 4; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Publish: &protocol.PublishRequest{Channel: "chat:a"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "chat command %d must be allowed", id)
	}
	sendRateLimitCommand(client, &protocol.Command{Id: 5, Publish: &protocol.PublishRequest{Channel: "chat:a"}})
	require.True(t, isRateLimited(transport.waitReply(t, 5)))

	// Channels of other namespaces use base buckets.
	sendRateLimitCommand(client, &protocol.Command{Id: 6, Publish: &protocol.PublishRequest{Channel: "other:a"}})
	require.False(t, isRateLimited(transport.waitReply(t, 6)))
	sendRateLimitCommand(client, &protocol.Command{Id: 7, Publish: &protocol.PublishRequest{Channel: "other:a"}})
	require.True(t, isRateLimited(transport.waitReply(t, 7)))
}

func TestRateLimitClientErrorDisconnect(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientError: configtypes.RateLimitClientError{
			Enabled: true,
			Total:   rateLimitBucketsOf(time.Minute, 3),
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	// Unknown namespace errors are protocol errors which are counted.
	for id := uint32(2); id <= 4; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Subscribe: &protocol.SubscribeRequest{Channel: "other:a"}})
		reply := transport.waitReply(t, id)
		require.NotNil(t, reply.Error)
	}
	require.False(t, transport.disconnected())

	sendRateLimitCommand(client, &protocol.Command{Id: 5, Subscribe: &protocol.SubscribeRequest{Channel: "other:a"}})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, uint32(3509), disconnect.Code)
	require.Equal(t, centrifuge.DisconnectTooManyErrors.Code, disconnect.Code)
}

func TestRateLimitClientErrorCountsRateLimitErrors(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Subscribe: rateLimitChannelOf(time.Minute, 1),
			},
		},
		ClientError: configtypes.RateLimitClientError{
			Enabled: true,
			Total:   rateLimitBucketsOf(time.Minute, 2),
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	// First error is an unknown channel, the next two are rate limit errors (111) which are counted too.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "other:a"}})
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Subscribe: &protocol.SubscribeRequest{Channel: "other:a"}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)))
	require.False(t, transport.disconnected())

	sendRateLimitCommand(client, &protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "other:a"}})
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectTooManyErrors.Code, disconnect.Code)
}

func TestRateLimitClientErrorInternalNotCounted(t *testing.T) {
	rl := configtypes.RateLimit{
		ClientError: configtypes.RateLimitClientError{
			Enabled: true,
			Total:   rateLimitBucketsOf(time.Minute, 2),
		},
	}
	node := newRateLimitHandler(t, rl)
	client, transport, _ := connectTestClient(t, node, 0)

	// Hook is called directly: internal errors are not produced by request handling in tests.
	hook := (&Handler{}).rateLimitCommandProcessed(ratelimit.NewClientLimiter(rl))
	for i := 0; i < 10; i++ {
		hook(client, centrifuge.CommandProcessedEvent{
			Command: &protocol.Command{Id: 2},
			Error:   centrifuge.ErrorInternal,
			Started: time.Now(),
		})
	}
	require.False(t, transport.disconnected())

	// Client errors are counted: the third one exceeds the burst of two.
	for i := 0; i < 3; i++ {
		hook(client, centrifuge.CommandProcessedEvent{
			Command: &protocol.Command{Id: 2},
			Error:   centrifuge.ErrorBadRequest,
			Started: time.Now(),
		})
	}
	disconnect := transport.waitDisconnect(t)
	require.Equal(t, centrifuge.DisconnectTooManyErrors.Code, disconnect.Code)
}

func TestRateLimitRepeatedConnectKeepsState(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Subscribe: rateLimitChannelOf(time.Minute, 1),
			},
		},
	})
	client, _, _ := connectTestClient(t, node, 0)
	state := client.ExtState()
	require.NotNil(t, state)

	// Repeated connect is rejected by Core, but it must not replace the connection limit state.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Connect: &protocol.ConnectRequest{}})
	require.Same(t, state, client.ExtState())
}

// TestRateLimitOperationMapping uses channels of namespace aa, registered as a shared poll namespace, and of bb, a plain one.
func TestRateLimitOperationMapping(t *testing.T) {
	h, _ := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{}, []configtypes.ChannelNamespace{
		{Name: "aa", ChannelOptions: configtypes.ChannelOptions{SubscriptionType: "shared_poll"}},
		{Name: "bb"},
	})
	tests := []struct {
		name    string
		cmd     *protocol.Command
		wantOp  ratelimit.Operation
		wantKey string
		wantOK  bool
	}{
		{name: "subscribe", cmd: &protocol.Command{Subscribe: &protocol.SubscribeRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationSubscribe, wantKey: "aa:b", wantOK: true},
		{name: "unsubscribe", cmd: &protocol.Command{Unsubscribe: &protocol.UnsubscribeRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationUnsubscribe, wantKey: "aa:b", wantOK: true},
		{name: "publish", cmd: &protocol.Command{Publish: &protocol.PublishRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationPublish, wantKey: "aa:b", wantOK: true},
		{name: "history", cmd: &protocol.Command{History: &protocol.HistoryRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationHistory, wantKey: "aa:b", wantOK: true},
		{name: "presence", cmd: &protocol.Command{Presence: &protocol.PresenceRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationPresence, wantKey: "aa:b", wantOK: true},
		{name: "presence_stats", cmd: &protocol.Command{PresenceStats: &protocol.PresenceStatsRequest{Channel: "aa:b"}}, wantOp: ratelimit.OperationPresenceStats, wantKey: "aa:b", wantOK: true},
		{
			name: "sub_refresh",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "aa:b"},
			},
			wantOp:  ratelimit.OperationSubRefresh,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "sub_refresh token type is not track",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "aa:b", Type: 0},
			},
			wantOp:  ratelimit.OperationSubRefresh,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "track",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "aa:b", Type: 1},
			},
			wantOp:  ratelimit.OperationTrack,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "untrack",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "aa:b", Type: 2},
			},
			wantOp:  ratelimit.OperationUntrack,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "track on a regular channel is sub_refresh",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "bb:c", Type: 1},
			},
			wantOp:  ratelimit.OperationSubRefresh,
			wantKey: "bb:c",
			wantOK:  true,
		},
		{
			name: "token refresh with a track type on a regular channel is sub_refresh",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "bb:c", Type: 2, Token: "token"},
			},
			wantOp:  ratelimit.OperationSubRefresh,
			wantKey: "bb:c",
			wantOK:  true,
		},
		{
			name: "track with a token on a shared poll channel is track",
			cmd: &protocol.Command{
				SubRefresh: &protocol.SubRefreshRequest{Channel: "aa:b", Type: 1, Token: "token"},
			},
			wantOp:  ratelimit.OperationTrack,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "map publish",
			cmd: &protocol.Command{
				Publish: &protocol.PublishRequest{Channel: "aa:b", Type: 1},
			},
			wantOp:  ratelimit.OperationMapPublish,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "map remove",
			cmd: &protocol.Command{
				Publish: &protocol.PublishRequest{Channel: "aa:b", Type: 1, Removed: true},
			},
			wantOp:  ratelimit.OperationMapRemove,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "regular publish with removed flag stays publish",
			cmd: &protocol.Command{
				Publish: &protocol.PublishRequest{Channel: "aa:b", Type: 0, Removed: true},
			},
			wantOp:  ratelimit.OperationPublish,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{
			name: "map subscribe is subscribe",
			cmd: &protocol.Command{
				Subscribe: &protocol.SubscribeRequest{Channel: "aa:b", Type: 1},
			},
			wantOp:  ratelimit.OperationSubscribe,
			wantKey: "aa:b",
			wantOK:  true,
		},
		{name: "refresh", cmd: &protocol.Command{Refresh: &protocol.RefreshRequest{}}, wantOp: ratelimit.OperationRefresh, wantOK: true},
		{name: "rpc", cmd: &protocol.Command{Rpc: &protocol.RPCRequest{Method: "m"}}, wantOp: ratelimit.OperationRPC, wantKey: "m", wantOK: true},
		{name: "ping not limited", cmd: &protocol.Command{Ping: &protocol.PingRequest{}}, wantOK: false},
		{name: "send not limited", cmd: &protocol.Command{Send: &protocol.SendRequest{}}, wantOK: false},
		{name: "connect not limited", cmd: &protocol.Command{Connect: &protocol.ConnectRequest{}}, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, key, ok := h.rateLimitOperation(tt.cmd)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			require.Equal(t, tt.wantOp, op)
			require.Equal(t, tt.wantKey, key)
		})
	}
}

func TestRateLimitMapOperationsOverLimit(t *testing.T) {
	_, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				MapPublish: rateLimitChannelOf(time.Minute, 1),
				MapRemove:  rateLimitChannelOf(time.Minute, 1),
				Track:      rateLimitChannelOf(time.Minute, 1),
				Untrack:    rateLimitChannelOf(time.Minute, 1),
			},
		},
	}, []configtypes.ChannelNamespace{
		{Name: "test"},
		{Name: "shared", ChannelOptions: configtypes.ChannelOptions{SubscriptionType: "shared_poll"}},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	tests := []struct {
		name   string
		op     string
		newCmd func(id uint32) *protocol.Command
	}{
		{
			name: "map publish",
			op:   "map_publish",
			newCmd: func(id uint32) *protocol.Command {
				return &protocol.Command{Id: id, Publish: &protocol.PublishRequest{Channel: "test:1", Type: 1}}
			},
		},
		{
			name: "map remove",
			op:   "map_remove",
			newCmd: func(id uint32) *protocol.Command {
				return &protocol.Command{Id: id, Publish: &protocol.PublishRequest{Channel: "test:1", Type: 1, Removed: true}}
			},
		},
		{
			name: "track",
			op:   "track",
			newCmd: func(id uint32) *protocol.Command {
				return &protocol.Command{Id: id, SubRefresh: &protocol.SubRefreshRequest{Channel: "shared:1", Type: 1}}
			},
		},
		{
			name: "untrack",
			op:   "untrack",
			newCmd: func(id uint32) *protocol.Command {
				return &protocol.Command{Id: id, SubRefresh: &protocol.SubRefreshRequest{Channel: "shared:1", Type: 2}}
			},
		},
	}
	id := uint32(2)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("client_command", tt.op))

			sendRateLimitCommand(client, tt.newCmd(id))
			require.False(t, isRateLimited(transport.waitReply(t, id)), "first command must be allowed")
			sendRateLimitCommand(client, tt.newCmd(id+1))
			require.True(t, isRateLimited(transport.waitReply(t, id+1)), "second command must be rejected")
			id += 2

			after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("client_command", tt.op))
			require.Equal(t, float64(1), after-before)
		})
	}
}

func TestRateLimitMapOperationsShareDefault(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Default: rateLimitBucketsOf(time.Minute, 1),
			},
		},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	// Map operations without their own buckets fall back to default, which is shared with regular publish.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Publish: &protocol.PublishRequest{Channel: "test:1", Type: 1}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Publish: &protocol.PublishRequest{Channel: "test:1"}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)))
	sendRateLimitCommand(client, &protocol.Command{Id: 4, SubRefresh: &protocol.SubRefreshRequest{Channel: "test:1", Type: 1}})
	require.True(t, isRateLimited(transport.waitReply(t, 4)))
}

func TestRateLimitSubRefreshTokenIsNotTrack(t *testing.T) {
	_, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Track:   rateLimitChannelOf(time.Minute, 1),
				Untrack: rateLimitChannelOf(time.Minute, 1),
			},
		},
	}, []configtypes.ChannelNamespace{
		{Name: "test"},
		{Name: "shared", ChannelOptions: configtypes.ChannelOptions{SubscriptionType: "shared_poll"}},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	// A token refresh of a channel which is not shared poll is sub_refresh whatever its type, so it does not
	// consume the track or untrack buckets.
	for id := uint32(2); id <= 4; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, SubRefresh: &protocol.SubRefreshRequest{Channel: "test:1", Type: 1, Token: "token"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "command %d must be allowed", id)
	}
}

// TestRateLimitTokenRefreshIsLimitedAsSubRefresh checks that a token refresh of a channel which is not shared poll is
// limited by sub_refresh when track and untrack are not set. Core runs the token refresh path for such a request.
func TestRateLimitTokenRefreshIsLimitedAsSubRefresh(t *testing.T) {
	_, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				SubRefresh: rateLimitChannelOf(time.Minute, 1),
			},
		},
	}, []configtypes.ChannelNamespace{
		{Name: "test"},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, SubRefresh: &protocol.SubRefreshRequest{Channel: "test:1", Type: 1, Token: "token"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)), "first token refresh must be allowed")
	sendRateLimitCommand(client, &protocol.Command{Id: 3, SubRefresh: &protocol.SubRefreshRequest{Channel: "test:1", Type: 1, Token: "token"}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)), "second token refresh must be rejected")
}

// TestRateLimitSharedPollTrackWithTokenIsTrack checks that a track on a shared poll channel is charged to track even when
// the request carries a token. Core routes such a request to track and ignores the token.
func TestRateLimitSharedPollTrackWithTokenIsTrack(t *testing.T) {
	_, node := newRateLimitHandlerWithNamespaces(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Track: rateLimitChannelOf(time.Minute, 1),
			},
		},
	}, []configtypes.ChannelNamespace{
		{Name: "shared", ChannelOptions: configtypes.ChannelOptions{SubscriptionType: "shared_poll"}},
	})
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, SubRefresh: &protocol.SubRefreshRequest{Channel: "shared:1", Type: 1, Token: "token"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)), "first track must be allowed")
	sendRateLimitCommand(client, &protocol.Command{Id: 3, SubRefresh: &protocol.SubRefreshRequest{Channel: "shared:1", Type: 1, Token: "token"}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)), "second track must be rejected")
}

// TestRateLimitMapPublishInheritsPublishBuckets checks that map publishes without buckets of their own count against
// the publish buckets, as before map publishes existed, so a publish limit cannot be bypassed with map publishes.
func TestRateLimitMapPublishInheritsPublishBuckets(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: rateLimitChannelOf(time.Minute, 1),
			},
		},
	}, "chat")
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Publish: &protocol.PublishRequest{Channel: "chat:1", Type: 1}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Publish: &protocol.PublishRequest{Channel: "chat:1", Type: 1}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)))
}

func TestRateLimitMapPublishNamespaceOverride(t *testing.T) {
	node := newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				MapPublish: configtypes.RateLimitChannelBuckets{
					Enabled: true,
					Buckets: rateLimitBucketsOf(time.Minute, 2).Buckets,
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{NamespaceName: "chat", Enabled: true, Buckets: rateLimitBucketsOf(time.Minute, 1).Buckets},
					},
				},
			},
		},
	}, "chat", "other")
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, &protocol.Command{Id: 2, Publish: &protocol.PublishRequest{Channel: "chat:1", Type: 1}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Publish: &protocol.PublishRequest{Channel: "chat:1", Type: 1}})
	require.True(t, isRateLimited(transport.waitReply(t, 3)))

	// Channels of other namespaces use the base buckets of map_publish.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Publish: &protocol.PublishRequest{Channel: "other:1", Type: 1}})
	require.False(t, isRateLimited(transport.waitReply(t, 4)))
	sendRateLimitCommand(client, &protocol.Command{Id: 5, Publish: &protocol.PublishRequest{Channel: "other:1", Type: 1}})
	require.False(t, isRateLimited(transport.waitReply(t, 5)))
	sendRateLimitCommand(client, &protocol.Command{Id: 6, Publish: &protocol.PublishRequest{Channel: "other:1", Type: 1}})
	require.True(t, isRateLimited(transport.waitReply(t, 6)))
}
