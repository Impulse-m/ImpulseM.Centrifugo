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
	"golang.org/x/sync/singleflight"
)

// CacheEmptyHandler answers cache empty events with the cache empty proxy named in the channel options.
type CacheEmptyHandler struct {
	proxies map[string]CacheEmptyProxy
	calls   map[string]*metrics.ProxyCallChildren
	// groups shares the in-flight call of each proxy between the subscribers of one channel.
	groups map[string]*singleflight.Group
	// logLimiter lets a failed call be logged at most once a minute.
	logLimiter *tools.IntervalLimiter
}

// NewCacheEmptyHandler creates a handler for the given cache empty proxies, by name.
func NewCacheEmptyHandler(proxies map[string]CacheEmptyProxy) *CacheEmptyHandler {
	// The handler keeps its own copy, so a later change of the caller's map cannot add a name without a group or a call.
	owned := maps.Clone(proxies)
	calls := make(map[string]*metrics.ProxyCallChildren, len(owned))
	groups := make(map[string]*singleflight.Group, len(owned))
	for name, p := range owned {
		calls[name] = metrics.NewProxyCallChildren(p.Protocol(), "cache_empty", name)
		groups[name] = &singleflight.Group{}
	}
	return &CacheEmptyHandler{
		proxies:    owned,
		calls:      calls,
		groups:     groups,
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
		// Subscribers that find the same channel empty while a call is in flight share its result, and the call
		// is logged once for all of them. groups has the same keys as proxies, so the lookup always succeeds.
		result, err, _ := h.groups[name].Do(e.Channel, func() (any, error) {
			populated, err := h.call(name, p, e.Channel)
			if err != nil && h.logLimiter.Allow() {
				log.Error().Err(err).Str("proxy", name).Str("channel", e.Channel).Msg("error proxying cache empty")
			}
			return populated, err
		})
		if err != nil {
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
	if resp == nil {
		return false, errors.New("cache empty proxy returned no response")
	}
	return resp.Result != nil && resp.Result.Populated, nil
}
