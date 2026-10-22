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
	deliveries := newDeliveryWithDropping(handlers, queueSize, inlineOps(), droppingOps())
	node.OnNotification(deliveries.deliver)
}

// droppingOps returns the ops which drop an event when their queue is full, instead of waiting for room. Their events are
// advisory: the usage stats are sent again with the next report, so a dropped update costs little, and the Core control
// reader never waits on them.
func droppingOps() map[string]bool {
	return map[string]bool{
		usage.LastSentUpdateNotificationOp: true,
	}
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
