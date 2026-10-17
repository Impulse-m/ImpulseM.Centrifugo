package api

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filterrequire"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"
)

// refusesRequiredServerTagsFilter reports whether a server API subscription to channel of user is refused by the
// require_server_tags_filter option of its namespace. The request has no server tags filter field, so the subscription
// carries none, and the namespace requires one. The refusal is logged with the route, the server API.
func refusesRequiredServerTagsFilter(chOpts *configtypes.ChannelOptions, channel string, user string) bool {
	if !chOpts.RequireServerTagsFilter {
		return false
	}
	filterrequire.LogRefused(channel, "", user, subsource.ServerAPI)
	return true
}
