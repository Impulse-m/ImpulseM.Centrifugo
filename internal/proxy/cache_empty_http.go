package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// HTTPCacheEmptyProxy sends cache empty requests to the application backend over HTTP.
type HTTPCacheEmptyProxy struct {
	config     Config
	httpCaller HTTPCaller
}

var _ CacheEmptyProxy = (*HTTPCacheEmptyProxy)(nil)

// NewHTTPCacheEmptyProxy creates a cache empty proxy which uses HTTP.
func NewHTTPCacheEmptyProxy(p Config) (*HTTPCacheEmptyProxy, error) {
	httpClient, err := proxyHTTPClient(p, "cache_empty_proxy")
	if err != nil {
		return nil, fmt.Errorf("error creating HTTP client: %w", err)
	}
	return &HTTPCacheEmptyProxy{
		httpCaller: NewHTTPCaller(httpClient),
		config:     p,
	}, nil
}

// ProxyNotifyCacheEmpty sends a cache empty request to application backend.
func (p *HTTPCacheEmptyProxy) ProxyNotifyCacheEmpty(ctx context.Context, req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	data, err := httpEncoder.EncodeNotifyCacheEmptyRequest(req)
	if err != nil {
		return nil, err
	}
	respData, err := p.httpCaller.CallHTTP(ctx, p.config.Endpoint, httpRequestHeaders(ctx, p.config), data)
	if err != nil {
		return nil, err
	}
	return httpDecoder.DecodeNotifyCacheEmptyResponse(respData)
}

// Protocol ...
func (p *HTTPCacheEmptyProxy) Protocol() string {
	return "http"
}

// Timeout ...
func (p *HTTPCacheEmptyProxy) Timeout() time.Duration {
	return p.config.Timeout.ToDuration()
}
