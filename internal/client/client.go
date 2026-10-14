package client

import (
	"context"

	"github.com/centrifugal/centrifuge"
)

// Client represents client connection.
type Client interface {
	ID() string
	UserID() string
	IsSubscribed(string) bool
	Context() context.Context
	Transport() centrifuge.TransportInfo
	AcquireStorage() (map[string]any, func(map[string]any))
	Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe)
	WritePublication(channel string, publication *centrifuge.Publication, sp centrifuge.StreamPosition) error
	Disconnect(disconnect ...centrifuge.Disconnect)
	ExtState() any
	Rebind(opts centrifuge.RebindOptions) error
	Subscribe(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsWithContext() map[string]centrifuge.ChannelContext
}
