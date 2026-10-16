package proxy

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// CacheEmptyHandler answers cache empty events with the cache empty proxy named in the channel options.
type CacheEmptyHandler struct {
	proxies map[string]CacheEmptyProxy
	calls   map[string]*metrics.ProxyCallChildren
	// logLimiter lets a failed call be logged at most once a minute.
	logLimiter *tools.IntervalLimiter
}

// NewCacheEmptyHandler creates a handler for the given cache empty proxies, by name.
func NewCacheEmptyHandler(proxies map[string]CacheEmptyProxy) *CacheEmptyHandler {
	// The handler keeps its own copy, so a later change of the caller's map cannot add a name without a call metric.
	owned := maps.Clone(proxies)
	calls := make(map[string]*metrics.ProxyCallChildren, len(owned))
	for name, p := range owned {
		calls[name] = metrics.NewProxyCallChildren(p.Protocol(), "cache_empty", name)
	}
	return &CacheEmptyHandler{
		proxies:    owned,
		calls:      calls,
		logLimiter: tools.NewIntervalLimiter(time.Minute),
	}
}

// Handle returns the cache empty handler of the Centrifuge node. The namespace decides whether the
// proxy is called: a channel whose options have cache_empty_proxy_enabled off gets an empty reply.
func (h *CacheEmptyHandler) Handle(cfgContainer *config.Container) centrifuge.CacheEmptyHandler {
	return func(e centrifuge.CacheEmptyEvent) (centrifuge.CacheEmptyReply, error) {
		// ChannelOptionsRef never returns an error.
		chOpts, found, _ := cfgContainer.ChannelOptionsRef(e.Channel)
		if !found || !chOpts.CacheEmptyProxyEnabled {
			return centrifuge.CacheEmptyReply{}, nil
		}

		name := chOpts.CacheEmptyProxyName
		p, ok := h.proxies[name]
		if !ok {
			log.Error().Str("proxy", name).Str("channel", e.Channel).Msg("cache empty proxy not found")
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		// Each subscriber makes its own call, so the answer is decided after its own arrival.
		populated, err := h.call(name, p, e.Channel)
		if err != nil {
			if h.logLimiter.Allow() {
				log.Error().Err(err).Str("proxy", name).Str("channel", e.Channel).Msg("error proxying cache empty")
			}
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		return centrifuge.CacheEmptyReply{
			Populated: populated,
		}, nil
	}
}

// call makes one cache empty call to the proxy p for the channel and returns whether the backend
// reports the channel as populated.
func (h *CacheEmptyHandler) call(name string, p CacheEmptyProxy, channel string) (bool, error) {
	// calls has the same keys as proxies, so the lookup by a name found in proxies always succeeds.
	end := h.calls[name].Track()
	ctx, cancel := context.WithTimeout(context.Background(), p.Timeout())
	defer cancel()

	resp, err := p.ProxyNotifyCacheEmpty(ctx, &proxyproto.NotifyCacheEmptyRequest{Channel: channel})
	if err == nil && resp == nil {
		err = errors.New("cache empty proxy returned no response")
	}
	end(err != nil)
	if err != nil {
		return false, err
	}
	return resp.Result != nil && resp.Result.Populated, nil
}
