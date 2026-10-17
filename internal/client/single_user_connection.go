package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
)

// enforceConnectSingleConnection enforces the single connection of the user of a connect at one of its two points, see
// OnClientConnecting. Without templates or the require option the single connection is enforced before the subscriptions
// of the connect are built, as upstream does. With either of them it is enforced after the subscriptions are checked, so a
// connect which a template or the option refuses does not disconnect the other connection of the user. The templates are
// read from the config the connect started with, and requiresFilter is the require option read by the connect, which the
// check after the subscriptions uses too, so only one of the two points enforces it.
func (h *Handler) enforceConnectSingleConnection(
	cfg *config.Config,
	requiresFilter bool,
	clientID string,
	credentials *centrifuge.Credentials,
	afterTemplates bool,
) error {
	deferred := cfg.HasServerTagsFilterTemplates() || requiresFilter
	if deferred != afterTemplates || !singleConnectionApplies(cfg, credentials) {
		return nil
	}
	return h.enforceSingleUserConnection(clientID, credentials.UserID)
}

// singleConnectionApplies reports whether a connect with credentials enforces the single connection of its user: the
// personal channel is subscribed, its single connection is set and the connection has a user.
func singleConnectionApplies(cfg *config.Config, credentials *centrifuge.Credentials) bool {
	return credentials != nil && cfg.Client.SubscribeToUserPersonalChannel.Enabled && cfg.Client.SubscribeToUserPersonalChannel.SingleConnection && credentials.UserID != ""
}
