package accessrevoke

import (
	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// HandleNotification applies an access revocation notification which a node received on NotifyOp. The event of this
// node is skipped, because the API call which sent it already applied the operation here. The expire_at of the
// notification is clamped to access_revoke.max_ttl before it is applied.
func HandleNotification(revoker *Revoker, localNodeID string, event centrifuge.NotificationEvent) {
	if event.FromNodeID == localNodeID {
		// Operation is already applied on this node by the API call.
		return
	}
	notification, err := DecodeNotification(event.Data)
	if err != nil {
		log.Error().Err(err).Str("from_node_id", event.FromNodeID).Msg("error decoding access revocation notification")
		return
	}
	notification = revoker.ClampToMaxTTL(notification, ClampSourceNotify)
	err = revoker.Apply(notification)
	if err != nil {
		log.Error().Err(err).Str("kind", notification.Kind).Msg("error applying access revocation notification")
	}
}
