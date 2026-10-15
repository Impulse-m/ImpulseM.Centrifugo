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

// GetChannelStateProxy creates the channel state proxy for the endpoint of p, HTTP or gRPC.
func GetChannelStateProxy(name string, p Config) (ChannelStateProxy, error) {
	normalizeProxyHeaderNames(&p)
	if isHttpEndpoint(p.Endpoint) {
		return NewHTTPChannelStateProxy(p)
	}
	return NewGRPCChannelStateProxy(name, p)
}
