package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// HTTPChannelStateProxy sends channel state events to the application backend over HTTP.
type HTTPChannelStateProxy struct {
	channelEventsHTTP
}

var _ ChannelStateProxy = (*HTTPChannelStateProxy)(nil)

// NewHTTPChannelStateProxy creates a channel state proxy which uses HTTP.
func NewHTTPChannelStateProxy(p Config) (ChannelStateProxy, error) {
	transport, err := newChannelEventsHTTP(p, "channel_state_proxy")
	if err != nil {
		return nil, err
	}
	return &HTTPChannelStateProxy{channelEventsHTTP: transport}, nil
}

// ProxyNotifyChannelState sends a batch of channel state events to application backend.
// A status_to_code_transforms rule is not applied: there is no client to send a code to.
// The headers come from a background context: a batch has no client, so no client headers are forwarded.
// A 200 response must carry a JSON object: an empty body fails to decode, and a null body is refused; the batch is sent again.
func (p *HTTPChannelStateProxy) ProxyNotifyChannelState(ctx context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	data, err := httpEncoder.EncodeNotifyChannelStateRequest(req)
	if err != nil {
		return nil, err
	}
	respData, err := p.httpCaller.CallHTTP(ctx, p.config.Endpoint, httpRequestHeaders(context.Background(), p.config), data)
	if err != nil {
		return nil, err
	}
	if isChannelEventsNullAnswer(respData) {
		return nil, errChannelEventsNullAnswer
	}
	return httpDecoder.DecodeNotifyChannelStateResponse(respData)
}
