package notify

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestAccessRevokeNotificationHandler(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	store := accessrevoke.NewStore(time.Minute)
	revoker := accessrevoke.NewRevoker(store, node.Hub(), accessrevoke.NewDisconnectQueue(1000, 100))
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
