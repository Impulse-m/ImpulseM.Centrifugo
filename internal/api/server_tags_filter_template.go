package api

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// refusesAPISubscribe reports whether a server API subscription to a channel with chOpts is refused. A server API
// subscription has no connection values to render a server tags filter template with, and a subscription without the
// template would bypass it.
func refusesAPISubscribe(chOpts *configtypes.ChannelOptions) bool {
	return chOpts.HasServerTagsFilterTemplate()
}
