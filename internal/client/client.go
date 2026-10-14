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
}

// rebindableClient is a Client whose connection state and channels the handler changes: the rebind of a refresh and
// the session index and rate limit state of a connection. Core's client satisfies it. A Client without these methods
// is only used by requests which do not change the connection.
type rebindableClient interface {
	Client
	Disconnect(disconnect ...centrifuge.Disconnect)
	ExtState() any
	Rebind(opts centrifuge.RebindOptions) error
	Subscribe(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsWithContext() map[string]centrifuge.ChannelContext
}
