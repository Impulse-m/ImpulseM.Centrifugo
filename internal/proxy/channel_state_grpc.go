package proxy

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc"
)

// GRPCChannelStateProxy sends channel state events to the application backend over gRPC.
type GRPCChannelStateProxy struct {
	config Config
	client proxyproto.CentrifugoProxyClient
}

var _ ChannelStateProxy = (*GRPCChannelStateProxy)(nil)

// NewGRPCChannelStateProxy creates a channel state proxy which uses gRPC.
func NewGRPCChannelStateProxy(name string, p Config) (*GRPCChannelStateProxy, error) {
	host, err := getGrpcHost(p.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("error getting grpc host: %v", err)
	}
	dialOpts, err := getDialOpts(name, p)
	if err != nil {
		return nil, fmt.Errorf("error creating GRPC dial options: %v", err)
	}
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to GRPC proxy server: %v", err)
	}
	return &GRPCChannelStateProxy{
		config: p,
		client: proxyproto.NewCentrifugoProxyClient(conn),
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
