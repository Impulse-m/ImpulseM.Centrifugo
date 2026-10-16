package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
)

// singleConnectionApplies reports whether a connect with credentials enforces the single connection of its user: the
// personal channel is subscribed, its single connection is set and the connection has a user. Connect checks it on both
// paths of the server tags filter templates, so the rule is written once here.
func singleConnectionApplies(cfg *config.Config, credentials *centrifuge.Credentials) bool {
	return credentials != nil && cfg.Client.SubscribeToUserPersonalChannel.Enabled && cfg.Client.SubscribeToUserPersonalChannel.SingleConnection && credentials.UserID != ""
}
