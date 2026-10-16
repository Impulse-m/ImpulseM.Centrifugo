package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// GRPCCacheEmptyProxy sends cache empty requests to the application backend over gRPC.
type GRPCCacheEmptyProxy struct {
	channelEventsGRPC
}

var _ CacheEmptyProxy = (*GRPCCacheEmptyProxy)(nil)

// NewGRPCCacheEmptyProxy creates a cache empty proxy which uses gRPC.
func NewGRPCCacheEmptyProxy(name string, p Config) (CacheEmptyProxy, error) {
	transport, err := newChannelEventsGRPC(name, p)
	if err != nil {
		return nil, err
	}
	return &GRPCCacheEmptyProxy{channelEventsGRPC: transport}, nil
}

// ProxyNotifyCacheEmpty sends a cache empty request to application backend.
func (p *GRPCCacheEmptyProxy) ProxyNotifyCacheEmpty(ctx context.Context, req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	return p.client.NotifyCacheEmpty(grpcRequestContext(ctx, p.config), req)
}
