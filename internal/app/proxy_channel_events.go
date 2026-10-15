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

// enableCacheEmptyProxies creates the cache empty proxies used by channels without namespace and by the
// namespaces with cache_empty_proxy_enabled, and records them in proxyMap.
func enableCacheEmptyProxies(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
) error {
	withoutNamespace := cfg.Channel.WithoutNamespace
	if withoutNamespace.CacheEmptyProxyEnabled {
		err := enableCacheEmptyProxy(proxyMap, cfg, namedProxies, withoutNamespace.CacheEmptyProxyName, "")
		if err != nil {
			return err
		}
	}
	for _, ns := range cfg.Channel.Namespaces {
		if !ns.CacheEmptyProxyEnabled {
			continue
		}
		err := enableCacheEmptyProxy(proxyMap, cfg, namedProxies, ns.CacheEmptyProxyName, ns.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkCacheEmptyReload rejects a reloaded config which enables a cache empty proxy that was not created at
// startup. The cache empty proxies and their hook are created once, at startup, so a reload can use only the
// proxies created then.
func checkCacheEmptyReload(proxyMap *client.ProxyMap, cfg config.Config) error {
	withoutNamespace := cfg.Channel.WithoutNamespace
	if withoutNamespace.CacheEmptyProxyEnabled {
		name := withoutNamespace.CacheEmptyProxyName
		if _, ok := proxyMap.CacheEmptyProxies[name]; !ok {
			return fmt.Errorf("cache empty proxy %s of channels without namespace was not created at startup, restart to use it", name)
		}
	}
	for _, ns := range cfg.Channel.Namespaces {
		if !ns.CacheEmptyProxyEnabled {
			continue
		}
		if _, ok := proxyMap.CacheEmptyProxies[ns.CacheEmptyProxyName]; !ok {
			return fmt.Errorf("cache empty proxy %s of namespace %s was not created at startup, restart to use it", ns.CacheEmptyProxyName, ns.Name)
		}
	}
	return nil
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
