package proxy

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

// CacheEmptyHandler answers cache empty events with the cache empty proxy named in the channel options.
type CacheEmptyHandler struct {
	proxies map[string]CacheEmptyProxy
	calls   map[string]*metrics.ProxyCallChildren
	group   singleflight.Group
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
		// Subscribers that find the same channel empty while a call is in flight share its result.
		key := name + "\x00" + e.Channel
		result, err, _ := h.group.Do(key, func() (any, error) {
			populated, err := h.call(name, p, e.Channel)
			return populated, err
		})
		if err != nil {
			log.Error().Err(err).Str("proxy", name).Str("channel", e.Channel).Msg("error proxying cache empty")
			return centrifuge.CacheEmptyReply{}, centrifuge.ErrorInternal
		}
		return centrifuge.CacheEmptyReply{
			Populated: result.(bool),
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
	end(err != nil)
	if err != nil {
		return false, err
	}
	return resp.Result != nil && resp.Result.Populated, nil
}
