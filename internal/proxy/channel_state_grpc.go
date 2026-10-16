package proxy

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc/metadata"
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
// The metadata comes from a background context: a batch has no client, so no client metadata is forwarded.
func (p *GRPCChannelStateProxy) ProxyNotifyChannelState(ctx context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	md, _ := metadata.FromOutgoingContext(grpcRequestContext(context.Background(), p.config))
	return p.client.NotifyChannelState(metadata.NewOutgoingContext(ctx, md), req)
}

// Protocol ...
func (p *GRPCChannelStateProxy) Protocol() string {
	return "grpc"
}

// Timeout ...
func (p *GRPCChannelStateProxy) Timeout() time.Duration {
	return p.config.Timeout.ToDuration()
}
