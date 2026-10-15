package proxy

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// CacheEmptyHandler answers cache empty events with the cache empty proxy named in the channel options.
type CacheEmptyHandler struct {
	proxies map[string]CacheEmptyProxy
	calls   map[string]*metrics.ProxyCallChildren
}

// NewCacheEmptyHandler creates a handler for the given cache empty proxies, by name.
func NewCacheEmptyHandler(proxies map[string]CacheEmptyProxy) *CacheEmptyHandler {
	calls := make(map[string]*metrics.ProxyCallChildren, len(proxies))
	for name, p := range proxies {
		calls[name] = metrics.NewProxyCallChildren(p.Protocol(), "cache_empty", name)
	}
	return &CacheEmptyHandler{
		proxies: proxies,
		calls:   calls,
	}
}

// Handle returns the cache empty handler of the Centrifuge node. The namespace decides whether the
// proxy is called: a channel whose options have cache_empty_proxy_enabled off gets an empty reply.
func (h *CacheEmptyHandler) Handle(cfgContainer *config.Container) centrifuge.CacheEmptyHandler {
	return func(e centrifuge.CacheEmptyEvent) (centrifuge.CacheEmptyReply, error) {
		chOpts, found, err := cfgContainer.ChannelOptionsRef(e.Channel)
		if err != nil {
			log.Error().Err(err).Str("channel", e.Channel).Msg("error getting channel options for cache empty")
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		if !found || !chOpts.CacheEmptyProxyEnabled {
			return centrifuge.CacheEmptyReply{}, nil
		}

		name := chOpts.CacheEmptyProxyName
		p, ok := h.proxies[name]
		if !ok {
			log.Error().Str("proxy", name).Str("channel", e.Channel).Msg("cache empty proxy not found")
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		calls := h.calls[name]
		if calls != nil {
			calls.Inflight.Inc()
			defer calls.Inflight.Dec()
		}

		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.Timeout())
		defer cancel()

		resp, err := p.ProxyNotifyCacheEmpty(ctx, &proxyproto.NotifyCacheEmptyRequest{Channel: e.Channel})
		duration := time.Since(started).Seconds()
		observeCacheEmptyCall(calls, duration, err != nil)
		if err != nil {
			log.Error().Err(err).Str("proxy", name).Str("channel", e.Channel).Msg("error proxying cache empty")
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		return centrifuge.CacheEmptyReply{
			Populated: resp.Result != nil && resp.Result.Populated,
		}, nil
	}
}

func observeCacheEmptyCall(calls *metrics.ProxyCallChildren, seconds float64, failed bool) {
	if calls == nil {
		return
	}
	calls.Duration.Observe(seconds)
	calls.Summary.Observe(seconds)
	if failed {
		calls.Errors.Inc()
	}
}
