package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newUserRateLimitHandler creates a handler with a real Core node which accepts connection tokens
// signed with "secret" and anonymous connections.
func newUserRateLimitHandler(t *testing.T, rl configtypes.RateLimit) *Handler {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.RateLimit = rl
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return h
}

// connectUserRateLimitClient connects a client authenticated as userID and returns it with its transport.
// The connect reply is not checked, so rejected connects can be tested too.
func connectUserRateLimitClient(t *testing.T, node *centrifuge.Node, userID string) (*centrifuge.Client, *recordingTransport) {
	t.Helper()
	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{Token: getConnTokenWithClaims(t, map[string]any{"sub": userID})},
	})
	return client, transport
}

func TestUserRateLimitSharedBetweenConnections(t *testing.T) {
	h := newUserRateLimitHandler(t, configtypes.RateLimit{
		UserCommand: configtypes.RateLimitUserCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Unsubscribe: rateLimitChannelOf(time.Minute, 2),
			},
		},
	})
	first, firstTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, firstTransport.waitReply(t, 1).Error)
	second, secondTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, secondTransport.waitReply(t, 1).Error)
	before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("user_command", "unsubscribe"))

	// The user budget of 2 is shared by both connections of the user.
	sendRateLimitCommand(first, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(firstTransport.waitReply(t, 2)))
	sendRateLimitCommand(second, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(secondTransport.waitReply(t, 2)))

	sendRateLimitCommand(first, &protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	reply := firstTransport.waitReply(t, 3)
	require.True(t, isRateLimited(reply))
	require.Equal(t, uint32(111), reply.Error.Code)

	sendRateLimitCommand(second, &protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.True(t, isRateLimited(secondTransport.waitReply(t, 3)))

	after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("user_command", "unsubscribe"))
	require.Equal(t, float64(2), after-before)

	// Another user is not affected.
	other, otherTransport := connectUserRateLimitClient(t, h.node, "43")
	require.Nil(t, otherTransport.waitReply(t, 1).Error)
	sendRateLimitCommand(other, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(otherTransport.waitReply(t, 2)))
}

func TestUserRateLimitAnonymousBypass(t *testing.T) {
	h := newUserRateLimitHandler(t, configtypes.RateLimit{
		UserCommand: configtypes.RateLimitUserCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Unsubscribe: rateLimitChannelOf(time.Minute, 1),
			},
		},
	})
	client, transport := connectRateLimitClient(t, h.node)
	for id := uint32(2); id <= 6; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "anonymous command %d must not be limited", id)
	}
}

func TestUserRateLimitConnectOverLimit(t *testing.T) {
	h := newUserRateLimitHandler(t, configtypes.RateLimit{
		UserCommand: configtypes.RateLimitUserCommand{
			Enabled: true,
			Connect: rateLimitBucketsOf(time.Minute, 1),
		},
	})
	before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("user_command", "connect"))

	_, firstTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, firstTransport.waitReply(t, 1).Error)

	_, secondTransport := connectUserRateLimitClient(t, h.node, "42")
	reply := secondTransport.waitReply(t, 1)
	require.NotNil(t, reply.Error)
	require.Equal(t, uint32(111), reply.Error.Code)

	after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("user_command", "connect"))
	require.Equal(t, float64(1), after-before)

	// Another user and anonymous connections are not limited by user connect.
	_, otherTransport := connectUserRateLimitClient(t, h.node, "43")
	require.Nil(t, otherTransport.waitReply(t, 1).Error)
	_, anonTransport := connectRateLimitClient(t, h.node)
	require.Nil(t, anonTransport.waitReply(t, 1).Error)
}

func TestUserRateLimitDisabledRegistersNothing(t *testing.T) {
	disabled := newUserRateLimitHandler(t, configtypes.RateLimit{})
	require.Nil(t, disabled.userLimiter)
	require.Empty(t, disabled.Services())

	enabled := newUserRateLimitHandler(t, configtypes.RateLimit{
		UserCommand: configtypes.RateLimitUserCommand{Enabled: true},
	})
	require.NotNil(t, enabled.userLimiter)
	require.Len(t, enabled.Services(), 1)
}
