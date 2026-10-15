package app

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/client"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/rs/zerolog/log"
)

// channelStateProxyEnabled reports whether channel state events are enabled for any namespace or
// for channels without namespace.
func channelStateProxyEnabled(cfg config.Config) bool {
	if cfg.Channel.WithoutNamespace.StateProxyEnabled {
		return true
	}
	for _, ns := range cfg.Channel.Namespaces {
		if ns.StateProxyEnabled {
			return true
		}
	}
	return false
}

// buildChannelStateProxy creates the channel state proxy of channel.proxy.state. The proxy is shared by
// all namespaces: one endpoint receives the events of all of them.
func buildChannelStateProxy(cfg config.Config) (proxy.ChannelStateProxy, error) {
	p := cfg.Channel.Proxy.State
	csp, err := proxy.GetChannelStateProxy("state", p)
	if err != nil {
		return nil, fmt.Errorf("error creating channel state proxy: %w", err)
	}
	log.Info().Str("endpoint", tools.RedactedLogURLs(p.Endpoint)[0]).Msg("channel state proxy configured")
	log.Warn().Msg("channel state events are not delivered by this build: the Redis wiring follows in the next release")
	return csp, nil
}

// resolveCacheEmptyProxy returns the config of the cache empty proxy named by name, the default one or
// one of the named proxies, and creates the cache empty proxy in proxyMap unless it exists already.
func resolveCacheEmptyProxy(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
	name string,
) (configtypes.Proxy, error) {
	var p configtypes.Proxy
	if name == config.DefaultProxyName {
		p = cfg.Channel.Proxy.CacheEmpty
	} else {
		var found bool
		p, found = namedProxies[name]
		if !found {
			return p, fmt.Errorf("cache empty proxy not found: %s", name)
		}
	}
	if _, ok := proxyMap.CacheEmptyProxies[name]; !ok {
		cep, err := proxy.GetCacheEmptyProxy(name, p)
		if err != nil {
			return p, fmt.Errorf("error creating cache empty proxy %s: %w", name, err)
		}
		proxyMap.CacheEmptyProxies[name] = cep
	}
	return p, nil
}
