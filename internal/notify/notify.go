package notify

import (
	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/usage"
	"github.com/rs/zerolog/log"
)

func RegisterHandlers(
	node *centrifuge.Node,
	statsSender *usage.Sender,
	revoker *accessrevoke.Revoker,
	extra map[string]centrifuge.NotificationHandler,
) {
	handlers := mergeHandlers(notificationHandlers(node, statsSender, revoker), extra)
	// The access revocation handler writes the store and enqueues its disconnects without blocking, so it runs inline:
	// the reader is never held by a full queue, and the events of the op keep their order.
	inline := map[string]bool{
		accessrevoke.NotifyOp: true,
	}
	deliveries := newDelivery(handlers, queueSize, inline)
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
			if event.FromNodeID == node.ID() {
				// Operation is already applied on this node by the API call.
				return
			}
			notification, err := accessrevoke.DecodeNotification(event.Data)
			if err != nil {
				log.Error().Err(err).Str("from_node_id", event.FromNodeID).Msg("error decoding access revocation notification")
				return
			}
			err = revoker.Apply(notification)
			if err != nil {
				log.Error().Err(err).Str("kind", notification.Kind).Msg("error applying access revocation notification")
			}
		},
	}
}
