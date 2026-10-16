package api

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// refusesAPISubscribe reports whether a server API subscription is refused on a channel with the options. A server API
// subscription has no connection whose values a server tags filter template is rendered with, and a subscription without
// the template would bypass it, so it is refused on a channel which has one.
func refusesAPISubscribe(chOpts *configtypes.ChannelOptions) bool {
	return chOpts.HasServerTagsFilterTemplate()
}
