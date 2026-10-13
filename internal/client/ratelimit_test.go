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

// newRateLimitHandler creates a handler with a real Core node and the given rate limit config.
func newRateLimitHandler(t *testing.T, rl configtypes.RateLimit, namespaces ...string) *centrifuge.Node {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.RateLimit = rl
	for _, name := range namespaces {
		cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{Name: name})
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{}, accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return node
}

// connectRateLimitClient connects an anonymous client and returns it with its transport.
func connectRateLimitClient(t *testing.T, node *centrifuge.Node) (*centrifuge.Client, *recordingTransport) {
	t.Helper()
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	reply := transport.waitReply(t, 1)
	require.Nil(t, reply.Error)
	return client, transport
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
	client, transport := connectRateLimitClient(t, node)
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
	other, otherTransport := connectRateLimitClient(t, node)
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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, transport := connectRateLimitClient(t, node)

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
	client, _ := connectRateLimitClient(t, node)
	state := client.ExtState()
	require.NotNil(t, state)

	// Repeated connect is rejected by Core, but it must not replace the connection limit state.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Connect: &protocol.ConnectRequest{}})
	require.Same(t, state, client.ExtState())
}

func TestRateLimitOperationMapping(t *testing.T) {
	tests := []struct {
		name    string
		cmd     *protocol.Command
		wantOp  ratelimit.Operation
		wantKey string
		wantOK  bool
	}{
		{name: "subscribe", cmd: &protocol.Command{Subscribe: &protocol.SubscribeRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationSubscribe, wantKey: "a:b", wantOK: true},
		{name: "unsubscribe", cmd: &protocol.Command{Unsubscribe: &protocol.UnsubscribeRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationUnsubscribe, wantKey: "a:b", wantOK: true},
		{name: "publish", cmd: &protocol.Command{Publish: &protocol.PublishRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationPublish, wantKey: "a:b", wantOK: true},
		{name: "history", cmd: &protocol.Command{History: &protocol.HistoryRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationHistory, wantKey: "a:b", wantOK: true},
		{name: "presence", cmd: &protocol.Command{Presence: &protocol.PresenceRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationPresence, wantKey: "a:b", wantOK: true},
		{name: "presence_stats", cmd: &protocol.Command{PresenceStats: &protocol.PresenceStatsRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationPresenceStats, wantKey: "a:b", wantOK: true},
		{name: "sub_refresh", cmd: &protocol.Command{SubRefresh: &protocol.SubRefreshRequest{Channel: "a:b"}}, wantOp: ratelimit.OperationSubRefresh, wantKey: "a:b", wantOK: true},
		{name: "refresh", cmd: &protocol.Command{Refresh: &protocol.RefreshRequest{}}, wantOp: ratelimit.OperationRefresh, wantOK: true},
		{name: "rpc", cmd: &protocol.Command{Rpc: &protocol.RPCRequest{Method: "m"}}, wantOp: ratelimit.OperationRPC, wantKey: "m", wantOK: true},
		{name: "ping not limited", cmd: &protocol.Command{Ping: &protocol.PingRequest{}}, wantOK: false},
		{name: "send not limited", cmd: &protocol.Command{Send: &protocol.SendRequest{}}, wantOK: false},
		{name: "connect not limited", cmd: &protocol.Command{Connect: &protocol.ConnectRequest{}}, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, key, ok := rateLimitOperation(tt.cmd)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			require.Equal(t, tt.wantOp, op)
			require.Equal(t, tt.wantKey, key)
		})
	}
}
