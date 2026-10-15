package proxy

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// GRPCCacheEmptyProxy sends cache empty requests to the application backend over gRPC.
type GRPCCacheEmptyProxy struct {
	config Config
	client proxyproto.CentrifugoProxyClient
}

var _ CacheEmptyProxy = (*GRPCCacheEmptyProxy)(nil)

// NewGRPCCacheEmptyProxy creates a cache empty proxy which uses gRPC.
func NewGRPCCacheEmptyProxy(name string, p Config) (*GRPCCacheEmptyProxy, error) {
	proxyClient, err := newGRPCProxyClient(name, p)
	if err != nil {
		return nil, err
	}
	return &GRPCCacheEmptyProxy{
		config: p,
		client: proxyClient,
	}, nil
}

// ProxyNotifyCacheEmpty sends a cache empty request to application backend.
func (p *GRPCCacheEmptyProxy) ProxyNotifyCacheEmpty(ctx context.Context, req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, p.config.Timeout.ToDuration())
	defer cancel()
	return p.client.NotifyCacheEmpty(grpcRequestContext(ctx, p.config), req)
}

// Protocol ...
func (p *GRPCCacheEmptyProxy) Protocol() string {
	return "grpc"
}

// Timeout ...
func (p *GRPCCacheEmptyProxy) Timeout() time.Duration {
	return p.config.Timeout.ToDuration()
}
