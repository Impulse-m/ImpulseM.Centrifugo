package proxy

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// ChannelStateHandler delivers batches of channel state events to the channel state proxy.
type ChannelStateHandler struct {
	proxy   ChannelStateProxy
	timeout time.Duration
	calls   *metrics.ProxyCallChildren
}

// NewChannelStateHandler creates a handler which calls the channel state proxy with the given timeout
// for every batch.
func NewChannelStateHandler(p ChannelStateProxy, timeout time.Duration) *ChannelStateHandler {
	return &ChannelStateHandler{
		proxy:   p,
		timeout: timeout,
		calls:   metrics.NewProxyCallChildren(p.Protocol(), "state", "default"),
	}
}

// Handle returns the function which delivers one batch of events. A non-nil error means the batch
// was not delivered and must be delivered again.
func (h *ChannelStateHandler) Handle() func(ctx context.Context, events []*proxyproto.ChannelEvent) error {
	return func(ctx context.Context, events []*proxyproto.ChannelEvent) error {
		end := h.calls.Track()
		ctx, cancel := context.WithTimeout(ctx, h.timeout)
		defer cancel()

		resp, err := h.proxy.ProxyNotifyChannelState(ctx, &proxyproto.NotifyChannelStateRequest{Events: events})
		if err == nil && resp.Error != nil {
			err = proxyproto.ErrorFromProto(resp.Error)
		}
		end(err != nil)
		return err
	}
}
