package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// GRPCChannelStateProxy sends channel state events to the application backend over gRPC.
type GRPCChannelStateProxy struct {
	config Config
	client proxyproto.CentrifugoProxyClient
}

var _ ChannelStateProxy = (*GRPCChannelStateProxy)(nil)

// NewGRPCChannelStateProxy creates a channel state proxy which uses gRPC.
func NewGRPCChannelStateProxy(name string, p Config) (*GRPCChannelStateProxy, error) {
	proxyClient, err := newGRPCProxyClient(name, p)
	if err != nil {
		return nil, err
	}
	return &GRPCChannelStateProxy{
		config: p,
		client: proxyClient,
	}, nil
}

// ProxyNotifyChannelState sends a batch of channel state events to application backend.
func (p *GRPCChannelStateProxy) ProxyNotifyChannelState(ctx context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, p.config.Timeout.ToDuration())
	defer cancel()
	return p.client.NotifyChannelState(grpcRequestContext(ctx, p.config), req)
}

// Protocol ...
func (p *GRPCChannelStateProxy) Protocol() string {
	return "grpc"
}
