package notify

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
// in the queue, then unblocks the user. Both events are delivered without waiting for the queue, the store is blocked
// after the block is delivered and unblocked after the unblock, and the dropped connection is kept because the retry
// finds the user unblocked. The inline handling itself is pinned by TestDeliveryRunsInlineOpOnDeliveringGoroutine.
func TestAccessRevokeBlockThenUnblockOnFullDisconnectQueue(t *testing.T) {
	node := newNotifyTestNode(t)
	store := accessrevoke.NewStore(time.Minute)
	queue := accessrevoke.NewDisconnectQueue(10000, 1)
	revoker := accessrevoke.NewRevokerWithMaxTTL(store, node.Hub(), queue, nil)
	deliveries := newDelivery(notificationHandlers(node, nil, revoker), queueSize)

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

	// The store is written before the disconnect is enqueued, so the block holds while the dropped disconnect waits.
	deliverWithin(t, deliveries, centrifuge.NotificationEvent{FromNodeID: "other-node", Op: accessrevoke.NotifyOp, Data: block})
	require.Equal(t, accessrevoke.Blocked, store.Check("user-1", centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict)

	deliverWithin(t, deliveries, centrifuge.NotificationEvent{FromNodeID: "other-node", Op: accessrevoke.NotifyOp, Data: unblock})
	require.Equal(t, accessrevoke.Allowed, store.Check("user-1", centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict)

	retries := testutil.ToFloat64(metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues("user"))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- queue.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	// The queued connection is disconnected by the queue, once. The dropped one is retried by user; the retry finds the
	// user unblocked and disconnects nothing, so the connection stays.
	require.Eventually(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues("user")) == retries+1
	}, 5*time.Second, 5*time.Millisecond)
	require.Len(t, node.Hub().UserConnections("user-1"), 1)
}

// deliverWithin delivers the event and fails the test if the delivery waits longer than a bound. An inline op returns at
// once, so the bound only catches a delivery which blocks on a full queue.
func deliverWithin(t *testing.T, deliveries *delivery, event centrifuge.NotificationEvent) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		deliveries.deliver(event)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery waited on the full disconnect queue")
	}
}

func TestAccessRevokeNotificationHandler(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	store := accessrevoke.NewStore(time.Minute)
	revoker := accessrevoke.NewRevokerWithMaxTTL(store, node.Hub(), accessrevoke.NewDisconnectQueue(1000, 100), nil)
	handlers := notificationHandlers(node, nil, revoker)
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
