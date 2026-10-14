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
	// The rebind path of a refresh moves the connection to another identity and changes its channels, so it needs
	// the methods below. Disconnect and ExtState serve the session index and the user rate limits.
	Rebind(opts centrifuge.RebindOptions) error
	Subscribe(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsWithContext() map[string]centrifuge.ChannelContext
}
