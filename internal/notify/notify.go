package notify

import (
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/usage"
	"github.com/rs/zerolog/log"
)

func RegisterHandlers(
	node *centrifuge.Node,
	statsSender *usage.Sender,
	revoker *accessrevoke.Revoker,
	cfgContainer *config.Container,
	extra map[string]centrifuge.NotificationHandler,
) {
	handlers := mergeHandlers(notificationHandlers(node, statsSender, revoker, cfgContainer), extra)
	deliveries := newDelivery(handlers, queueSize)
	node.OnNotification(deliveries.deliver)
}

func notificationHandlers(
	node *centrifuge.Node,
	statsSender *usage.Sender,
	revoker *accessrevoke.Revoker,
	cfgContainer *config.Container,
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
			maxTTL := cfgContainer.Config().AccessRevoke.MaxTTL.ToDuration()
			notification = accessrevoke.ClampExpireAt(notification, time.Now(), maxTTL, accessrevoke.ClampSourceNotify)
			err = revoker.Apply(notification)
			if err != nil {
				log.Error().Err(err).Str("kind", notification.Kind).Msg("error applying access revocation notification")
			}
		},
	}
}
