package notify

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// newNotifyTestNode creates a node which takes the user ID from the connect token.
func newNotifyTestNode(t *testing.T) *centrifuge.Node {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: e.Token},
		}, nil
	})
	return node
}

func connectNotifyTestClient(t *testing.T, node *centrifuge.Node, userID string) {
	t.Helper()
	transport := tools.NewTestTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{Token: userID},
	}
	data, err := protocol.NewJSONCommandEncoder().Encode(command)
	require.NoError(t, err)
	require.True(t, centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20))
}

// TestAccessRevokeBlockThenUnblockOnFullDisconnectQueue blocks a user whose second connection's disconnect does not fit
// in the queue, then unblocks the user. Both events are delivered without waiting for the queue, the store ends
// unblocked, and the dropped connection is kept because the retry finds the user unblocked.
func TestAccessRevokeBlockThenUnblockOnFullDisconnectQueue(t *testing.T) {
	node := newNotifyTestNode(t)
	store := accessrevoke.NewStore(time.Minute)
	queue := accessrevoke.NewDisconnectQueue(10000, 1)
	revoker := accessrevoke.NewRevoker(store, node.Hub(), queue)
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	deliveries := newDelivery(notificationHandlers(node, nil, revoker, cfgContainer), queueSize)

	connectNotifyTestClient(t, node, "user-1")
	connectNotifyTestClient(t, node, "user-1")
	require.Eventually(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 2
	}, 5*time.Second, 5*time.Millisecond)

	block, err := accessrevoke.EncodeNotification(accessrevoke.Notification{
		Kind:     accessrevoke.KindBlockUser,
		User:     "user-1",
		ExpireAt: time.Now().Add(time.Minute).Unix(),
	})
	require.NoError(t, err)
	unblock, err := accessrevoke.EncodeNotification(accessrevoke.Notification{
		Kind: accessrevoke.KindUnblockUser,
		User: "user-1",
	})
	require.NoError(t, err)

	delivered := make(chan struct{})
	go func() {
		deliveries.deliver(centrifuge.NotificationEvent{FromNodeID: "other-node", Op: accessrevoke.NotifyOp, Data: block})
		deliveries.deliver(centrifuge.NotificationEvent{FromNodeID: "other-node", Op: accessrevoke.NotifyOp, Data: unblock})
		close(delivered)
	}()
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery of block and unblock waited on the full disconnect queue")
	}
	require.Equal(t, accessrevoke.Allowed, store.Check("user-1", centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- queue.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	// The queued connection is disconnected by the queue. The dropped one is retried and stays connected.
	require.Eventually(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 0
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestAccessRevokeNotificationHandler(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	store := accessrevoke.NewStore(time.Minute)
	revoker := accessrevoke.NewRevoker(store, node.Hub(), accessrevoke.NewDisconnectQueue(1000, 100))
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	handlers := notificationHandlers(node, nil, revoker, cfgContainer)
	handler := handlers[accessrevoke.NotifyOp]
	require.NotNil(t, handler)

	data, err := accessrevoke.EncodeNotification(accessrevoke.Notification{
		Kind:     accessrevoke.KindBlockUser,
		User:     "user",
		ExpireAt: time.Now().Add(time.Minute).Unix(),
	})
	require.NoError(t, err)

	// Event from this node is ignored: it has already been applied by the API call.
	handler(centrifuge.NotificationEvent{FromNodeID: node.ID(), Op: accessrevoke.NotifyOp, Data: data})
	require.Equal(t, accessrevoke.Allowed, store.Check("user", centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict)

	// Event from another node is applied locally.
	handler(centrifuge.NotificationEvent{FromNodeID: "other-node", Op: accessrevoke.NotifyOp, Data: data})
	require.Equal(t, accessrevoke.Blocked, store.Check("user", centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict)
}
