package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// connectionTokenFiltersKey is a client storage key with server tags filters of channels
// subscribed by connection token, as they were applied on connect.
const connectionTokenFiltersKey = "connection_token_server_tags_filters"

// connectionTokenFilters returns server tags filters of connection token subscriptions, or nil when
// none of them has a filter. Core keeps no filter per channel, so the filters are stored on connect
// to detect their change on refresh. Connections without filters store nothing.
func connectionTokenFilters(subs map[string]centrifuge.SubscribeOptions) map[string]*protocol.FilterNode {
	var filters map[string]*protocol.FilterNode
	for ch, opts := range subs {
		if opts.Source != subsource.ConnectionToken || opts.ServerTagsFilter == nil {
			continue
		}
		if filters == nil {
			filters = make(map[string]*protocol.FilterNode)
		}
		filters[ch] = opts.ServerTagsFilter
	}
	return filters
}
