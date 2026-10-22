package notify

import (
	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/usage"
)

func RegisterHandlers(
	node *centrifuge.Node,
	statsSender *usage.Sender,
	revoker *accessrevoke.Revoker,
	extra map[string]centrifuge.NotificationHandler,
) {
	handlers := mergeHandlers(notificationHandlers(node, statsSender, revoker), extra)
	deliveries := newDelivery(handlers, queueSize, inlineOps())
	node.OnNotification(deliveries.deliver)
}

func notificationHandlers(
	node *centrifuge.Node,
	statsSender *usage.Sender,
	revoker *accessrevoke.Revoker,
) map[string]centrifuge.NotificationHandler {
	return map[string]centrifuge.NotificationHandler{
		usage.LastSentUpdateNotificationOp: func(event centrifuge.NotificationEvent) {
			if statsSender == nil {
				// Different configurations on different nodes.
				return
			}
			statsSender.UpdateLastSentAt(event.Data)
		},
		accessrevoke.NotifyOp: func(event centrifuge.NotificationEvent) {
			accessrevoke.HandleNotification(revoker, node.ID(), event)
		},
	}
}
