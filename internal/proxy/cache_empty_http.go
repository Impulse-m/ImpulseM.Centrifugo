package proxy

import (
	"context"
	"encoding/json"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// HTTPCacheEmptyProxy sends cache empty requests to the application backend over HTTP.
type HTTPCacheEmptyProxy struct {
	channelEventsHTTP
}

var _ CacheEmptyProxy = (*HTTPCacheEmptyProxy)(nil)

// NewHTTPCacheEmptyProxy creates a cache empty proxy which uses HTTP.
func NewHTTPCacheEmptyProxy(p Config) (CacheEmptyProxy, error) {
	transport, err := newChannelEventsHTTP(p, "cache_empty_proxy")
	if err != nil {
		return nil, err
	}
	return &HTTPCacheEmptyProxy{channelEventsHTTP: transport}, nil
}

// cacheEmptyEnvelope holds the error member of a cache empty HTTP answer. The result is decoded by
// DecodeNotifyCacheEmptyResponse, which has no error member.
type cacheEmptyEnvelope struct {
	Error *proxyproto.Error `json:"error"`
}

// ProxyNotifyCacheEmpty sends a cache empty request to application backend. An answer with an error
// member fails the call, even with status 200.
func (p *HTTPCacheEmptyProxy) ProxyNotifyCacheEmpty(ctx context.Context, req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	data, err := httpEncoder.EncodeNotifyCacheEmptyRequest(req)
	if err != nil {
		return nil, err
	}
	respData, err := p.httpCaller.CallHTTP(ctx, p.config.Endpoint, httpRequestHeaders(ctx, p.config), data)
	if err != nil {
		return nil, err
	}
	var envelope cacheEmptyEnvelope
	err = json.Unmarshal(respData, &envelope)
	if err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, proxyproto.ErrorFromProto(envelope.Error)
	}
	return httpDecoder.DecodeNotifyCacheEmptyResponse(respData)
}
