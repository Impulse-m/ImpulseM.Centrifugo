package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc/metadata"
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
// The metadata comes from a background context: a batch has no client, so no client metadata is forwarded.
func (p *GRPCChannelStateProxy) ProxyNotifyChannelState(ctx context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	md, _ := metadata.FromOutgoingContext(grpcRequestContext(context.Background(), p.config))
	return p.client.NotifyChannelState(metadata.NewOutgoingContext(ctx, md), req)
}
