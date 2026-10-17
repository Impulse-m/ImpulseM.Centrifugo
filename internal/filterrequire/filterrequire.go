// Package filterrequire holds the check of the require_server_tags_filter channel option: whether a subscription must be
// refused for carrying no server tags filter, the names of the routes which made the subscription, and the log line of
// a refusal. The client and the server API checks use it, so the message exists once.
package filterrequire

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/logging"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
)

// Missing reports whether a subscription to a channel with chOpts must be refused: the namespace requires a server tags
// filter and the subscription carries none. A non-nil filter is not validated here, Core refuses an invalid one.
func Missing(chOpts *configtypes.ChannelOptions, filter *protocol.FilterNode) bool {
	return chOpts.RequireServerTagsFilter && filter == nil
}

// LogRefused logs a refusal of a subscription with the route which forgot the filter. Warn for a route the backend
// asserts. Debug for a channel the client chose at connect (UniConnect), which any client can trigger on every connect.
func LogRefused(channel string, clientID string, userID string, source uint8) {
	if source == subsource.UniConnect {
		if logging.Enabled(logging.DebugLevel) {
			log.Debug().Str("channel", channel).Str("client", clientID).Str("user", userID).Str("source", SourceName(source)).Msg("server tags filter required, subscription refused")
		}
		return
	}
	log.Warn().Str("channel", channel).Str("client", clientID).Str("user", userID).Str("source", SourceName(source)).Msg("server tags filter required, subscription refused")
}

// LogHistoryRefused logs a refused client history call on a namespace which requires a server tags filter.
func LogHistoryRefused(channel string, clientID string, userID string) {
	if logging.Enabled(logging.DebugLevel) {
		log.Debug().Str("channel", channel).Str("client", clientID).Str("user", userID).Msg("server tags filter required, history refused")
	}
}

// SourceName returns the snake_case name of the subscription source, as the log line names it.
func SourceName(source uint8) string {
	switch source {
	case subsource.ConnectionToken:
		return "connection_token"
	case subsource.ConnectProxy:
		return "connect_proxy"
	case subsource.SubscriptionToken:
		return "subscription_token"
	case subsource.SubscribeProxy:
		return "subscribe_proxy"
	case subsource.UserLimited:
		return "user_limited"
	case subsource.ClientAllowed:
		return "client_allowed"
	case subsource.ClientInsecure:
		return "client_insecure"
	case subsource.UniConnect:
		return "uni_connect"
	case subsource.UserPersonal:
		return "user_personal"
	case subsource.ServerAPI:
		return "server_api"
	case subsource.StreamProxy:
		return "stream_proxy"
	}
	return "unknown"
}
