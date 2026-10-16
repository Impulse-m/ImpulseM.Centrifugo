package proxy

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/rs/zerolog/log"
)

// channelStateBatchKey identifies a batch by its first event. A batch which is sent again starts with the
// same event, because the events of a partition are delivered in order.
type channelStateBatchKey struct {
	channel   string
	eventType string
	timeMs    int64
}

// ChannelStateHandler delivers batches of channel state events to the channel state proxy. The oldest failing
// batch gauge is process wide, so one handler is supported per process.
type ChannelStateHandler struct {
	proxy   ChannelStateProxy
	timeout time.Duration
	calls   *metrics.ProxyCallChildren
	// logLimiter lets a rejected batch be logged at most once a minute.
	logLimiter *tools.IntervalLimiter
	// mu guards failing.
	mu sync.Mutex
	// failing maps the first event of each rejected batch to the time the batch was first sent.
	// An entry is removed when the batch is delivered or forgotten.
	failing map[channelStateBatchKey]time.Time
}

// NewChannelStateHandler creates a handler which calls the channel state proxy for every batch, bounded by
// the timeout of the proxy.
func NewChannelStateHandler(p ChannelStateProxy) *ChannelStateHandler {
	return &ChannelStateHandler{
		proxy:      p,
		timeout:    p.Timeout(),
		calls:      metrics.NewProxyCallChildren(p.Protocol(), "state", "default"),
		logLimiter: tools.NewIntervalLimiter(time.Minute),
		failing:    map[channelStateBatchKey]time.Time{},
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
		if err == nil && resp == nil {
			err = errors.New("channel state proxy returned no response")
		}
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

// Forget removes the rejected batch of events from the gauge. The partition consumer calls it when it stops
// delivering the batch, for example when another node takes over the partition, so the gauge does not show a
// batch which this node no longer retries.
func (h *ChannelStateHandler) Forget(events []*proxyproto.ChannelEvent) {
	if len(events) == 0 {
		return
	}
	key := newChannelStateBatchKey(events[0])
	h.mu.Lock()
	delete(h.failing, key)
	h.updateGaugeLocked()
	h.mu.Unlock()
}

// track records the outcome of a batch whose first event is first. A rejected batch keeps the time it was
// first sent until a later attempt delivers it, so a batch which is never accepted shows as a growing age.
func (h *ChannelStateHandler) track(first *proxyproto.ChannelEvent, size int, err error) {
	key := newChannelStateBatchKey(first)
	h.mu.Lock()
	_, failing := h.failing[key]
	switch {
	case err == nil && failing:
		delete(h.failing, key)
		h.updateGaugeLocked()
	case err != nil && !failing:
		h.failing[key] = time.Now()
		h.updateGaugeLocked()
	}
	h.mu.Unlock()

	if err != nil && h.logLimiter.Allow() {
		log.Error().
			Err(err).
			Str("channel", first.Channel).
			Str("type", first.Type).
			Int("events", size).
			Msg("channel state batch rejected, it is sent again and the events behind it wait")
	}
}

// newChannelStateBatchKey returns the key of the batch whose first event is first.
func newChannelStateBatchKey(first *proxyproto.ChannelEvent) channelStateBatchKey {
	return channelStateBatchKey{channel: first.Channel, eventType: first.Type, timeMs: first.TimeMs}
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
