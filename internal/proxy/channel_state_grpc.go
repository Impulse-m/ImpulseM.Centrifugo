package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// GRPCChannelStateProxy sends channel state events to the application backend over gRPC.
type GRPCChannelStateProxy struct {
	channelEventsGRPC
}

var _ ChannelStateProxy = (*GRPCChannelStateProxy)(nil)

// NewGRPCChannelStateProxy creates a channel state proxy which uses gRPC.
func NewGRPCChannelStateProxy(name string, p Config) (ChannelStateProxy, error) {
	transport, err := newChannelEventsGRPC(name, p)
	if err != nil {
		return nil, err
	}
	return &GRPCChannelStateProxy{channelEventsGRPC: transport}, nil
}

// ProxyNotifyChannelState sends a batch of channel state events to application backend.
// A batch has no client, so the context carries no client metadata; only the static metadata of the proxy is sent.
func (p *GRPCChannelStateProxy) ProxyNotifyChannelState(ctx context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	return p.client.NotifyChannelState(grpcRequestContext(ctx, p.config), req)
}
