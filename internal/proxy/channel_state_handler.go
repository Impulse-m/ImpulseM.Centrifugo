package proxy

import (
	"context"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// channelStateBatchKey identifies a batch by its first event. A batch which is sent again starts with the
// same event, because the events of a partition are delivered in order.
type channelStateBatchKey struct {
	channel   string
	eventType string
	timeMs    int64
}

// ChannelStateHandler delivers batches of channel state events to the channel state proxy.
type ChannelStateHandler struct {
	proxy   ChannelStateProxy
	timeout time.Duration
	calls   *metrics.ProxyCallChildren
	// logger logs a rejected batch at most once a minute.
	logger zerolog.Logger
	// mu guards failing.
	mu sync.Mutex
	// failing maps the first event of each rejected batch to the time the batch was first sent.
	// An entry is removed when the batch is delivered.
	failing map[channelStateBatchKey]time.Time
}

// NewChannelStateHandler creates a handler which calls the channel state proxy for every batch, bounded by
// the timeout of the proxy.
func NewChannelStateHandler(p ChannelStateProxy) *ChannelStateHandler {
	return &ChannelStateHandler{
		proxy:   p,
		timeout: p.Timeout(),
		calls:   metrics.NewProxyCallChildren(p.Protocol(), "state", "default"),
		logger:  log.Logger.Sample(&zerolog.BurstSampler{Burst: 1, Period: time.Minute}),
		failing: map[channelStateBatchKey]time.Time{},
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
		if len(events) > 0 {
			h.track(events[0], len(events), err)
		}
		return err
	}
}

// track records the outcome of a batch whose first event is first. A rejected batch keeps the time it was
// first sent until a later attempt delivers it, so a batch which is never accepted shows as a growing age.
func (h *ChannelStateHandler) track(first *proxyproto.ChannelEvent, size int, err error) {
	key := channelStateBatchKey{channel: first.Channel, eventType: first.Type, timeMs: first.TimeMs}
	h.mu.Lock()
	_, failing := h.failing[key]
	if err == nil {
		delete(h.failing, key)
	} else if !failing {
		h.failing[key] = time.Now()
	}
	h.updateGaugeLocked()
	h.mu.Unlock()

	if err != nil {
		h.logger.Error().
			Err(err).
			Str("channel", first.Channel).
			Str("type", first.Type).
			Int("events", size).
			Msg("channel state batch rejected, it is sent again and the events behind it wait")
	}
}

// updateGaugeLocked sets the gauge to the time of the oldest rejected batch, or to 0 when no batch is rejected.
// The caller holds mu.
func (h *ChannelStateHandler) updateGaugeLocked() {
	if metrics.ChannelStateOldestFailingBatchTimestamp == nil {
		return
	}
	var oldest time.Time
	for _, sent := range h.failing {
		if oldest.IsZero() || sent.Before(oldest) {
			oldest = sent
		}
	}
	var timestamp float64
	if !oldest.IsZero() {
		timestamp = float64(oldest.UnixNano()) / float64(time.Second)
	}
	metrics.ChannelStateOldestFailingBatchTimestamp.Set(timestamp)
}
