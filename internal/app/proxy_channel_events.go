package app

import (
	"errors"
	"fmt"
	"maps"

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
// all namespaces: one endpoint receives the events of all of them. The presence manager does not feed
// this proxy in this build yet, so no channel state event is sent.
func buildChannelStateProxy(cfg config.Config) (proxy.ChannelStateProxy, error) {
	csp, err := proxy.GetChannelStateProxy("state", cfg.Channel.Proxy.State)
	if err != nil {
		return nil, fmt.Errorf("error creating channel state proxy: %w", err)
	}
	log.Warn().Msg("no channel state events are sent: the presence manager is not wired to the channel state proxy")
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

// enableChannelEventProxies creates the channel state proxy, when a namespace enables channel state events, and
// the cache empty proxies, and records them in proxyMap.
func enableChannelEventProxies(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
) error {
	if channelStateProxyEnabled(cfg) {
		csp, err := buildChannelStateProxy(cfg)
		if err != nil {
			return err
		}
		proxyMap.ChannelStateProxy = csp
	}
	return enableCacheEmptyProxies(proxyMap, cfg, namedProxies)
}

// enableCacheEmptyProxies creates the cache empty proxies used by channels without namespace and by the
// namespaces with cache_empty_proxy_enabled, and records them in proxyMap.
func enableCacheEmptyProxies(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
) error {
	return forEachCacheEmptyUser(cfg, func(name string, namespace string) error {
		return enableCacheEmptyProxy(proxyMap, cfg, namedProxies, name, namespace)
	})
}

// forEachCacheEmptyUser calls fn with the cache empty proxy name and the namespace of each channel group that
// has a cache empty proxy enabled. The group of channels without namespace has the empty namespace. It stops at
// the first error fn returns.
func forEachCacheEmptyUser(cfg config.Config, fn func(name string, namespace string) error) error {
	withoutNamespace := cfg.Channel.WithoutNamespace
	if withoutNamespace.CacheEmptyProxyEnabled {
		if err := fn(withoutNamespace.CacheEmptyProxyName, ""); err != nil {
			return err
		}
	}
	for _, ns := range cfg.Channel.Namespaces {
		if !ns.CacheEmptyProxyEnabled {
			continue
		}
		if err := fn(ns.CacheEmptyProxyName, ns.Name); err != nil {
			return err
		}
	}
	return nil
}

// cacheEmptyLayout returns the cache empty proxy name of each enabled channel group, keyed by namespace. Channels
// without namespace are keyed by the empty string.
func cacheEmptyLayout(cfg config.Config) map[string]string {
	layout := map[string]string{}
	// The callback never fails, so the error of forEachCacheEmptyUser is always nil.
	_ = forEachCacheEmptyUser(cfg, func(name string, namespace string) error {
		layout[namespace] = name
		return nil
	})
	return layout
}

// checkCacheEmptyReload rejects a reloaded config which changes the cache empty layout of the startup config: a
// cache empty proxy enabled or disabled, added, removed or renamed. The cache empty proxies and their hook are
// created once, at startup, so only the options of the proxies can change on reload. The endpoint and timeout of
// a cache empty proxy do not change on reload either.
func checkCacheEmptyReload(startup map[string]string, newCfg config.Config) error {
	if maps.Equal(startup, cacheEmptyLayout(newCfg)) {
		return nil
	}
	return errors.New("cache empty proxies changed by reload: enabling, disabling, adding, removing or renaming one needs a restart")
}

// enableCacheEmptyProxy resolves the cache empty proxy named by name and logs it. An empty namespace
// means channels without namespace.
func enableCacheEmptyProxy(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
	name string,
	namespace string,
) error {
	p, err := resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, name)
	if err != nil {
		return err
	}
	event := log.Info().Str("proxy_name", name).Str("endpoint", tools.RedactedLogURLs(p.Endpoint)[0])
	if namespace == "" {
		event.Msg("cache empty proxy enabled for channels without namespace")
		return nil
	}
	event.Str("namespace", namespace).Msg("cache empty proxy enabled for channels in namespace")
	return nil
}
