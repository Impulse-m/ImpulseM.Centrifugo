package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// ChannelStateProxy allows to send channel state events (occupied, vacated) to the application backend.
type ChannelStateProxy interface {
	ProxyNotifyChannelState(context.Context, *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error)
	// Protocol for metrics and logging.
	Protocol() string
}
