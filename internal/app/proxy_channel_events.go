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

// cacheEmptyProxyConfig returns the config of the cache empty proxy named by name: the default one, or the named
// proxy. The bool is false when no proxy has that name.
func cacheEmptyProxyConfig(
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
	name string,
) (configtypes.Proxy, bool) {
	if name == config.DefaultProxyName {
		return cfg.Channel.Proxy.CacheEmpty, true
	}
	p, found := namedProxies[name]
	return p, found
}

// resolveCacheEmptyProxy returns the config of the cache empty proxy named by name, the default one or
// one of the named proxies, and creates the cache empty proxy in proxyMap unless it exists already.
func resolveCacheEmptyProxy(
	proxyMap *client.ProxyMap,
	cfg config.Config,
	namedProxies map[string]configtypes.Proxy,
	name string,
) (configtypes.Proxy, error) {
	p, found := cacheEmptyProxyConfig(cfg, namedProxies, name)
	if !found {
		return p, fmt.Errorf("cache empty proxy not found: %s", name)
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
	return forEachCacheEmptyGroup(cfg, func(name string, namespace string) error {
		return enableCacheEmptyProxy(proxyMap, cfg, namedProxies, name, namespace)
	})
}

// forEachCacheEmptyGroup calls fn with the cache empty proxy name and the namespace of each channel group that
// has a cache empty proxy enabled. The group of channels without namespace has the empty namespace. It stops at
// the first error fn returns.
func forEachCacheEmptyGroup(cfg config.Config, fn func(name string, namespace string) error) error {
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
	// The callback never fails, so the error of forEachCacheEmptyGroup is always nil.
	_ = forEachCacheEmptyGroup(cfg, func(name string, namespace string) error {
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
	return errors.New("cache empty proxies changed by reload: enabling, disabling, adding, removing or renaming one needs a restart, and the whole reload is refused until then")
}

// cacheEmptyTarget is the endpoint and the timeout of a cache empty proxy, as the config sets them.
type cacheEmptyTarget struct {
	endpoint string
	timeout  configtypes.Duration
}

// cacheEmptyTargets returns the target of each cache empty proxy of the enabled channel groups, keyed by the
// proxy name. The proxies are created from these targets at startup.
func cacheEmptyTargets(cfg config.Config) map[string]cacheEmptyTarget {
	named := make(map[string]configtypes.Proxy, len(cfg.Proxies))
	for _, p := range cfg.Proxies {
		named[p.Name] = p.Proxy
	}
	targets := map[string]cacheEmptyTarget{}
	// The callback never fails, so the error of forEachCacheEmptyGroup is always nil.
	_ = forEachCacheEmptyGroup(cfg, func(name string, _ string) error {
		p, _ := cacheEmptyProxyConfig(cfg, named, name)
		targets[name] = cacheEmptyTarget{endpoint: p.Endpoint, timeout: p.Timeout}
		return nil
	})
	return targets
}

// warnCacheEmptyTargetsReload logs a warning for each cache empty proxy whose endpoint or timeout differs in the
// reloaded config. A reload keeps the proxies created at startup, so the new values take effect on restart.
func warnCacheEmptyTargetsReload(startup map[string]cacheEmptyTarget, newCfg config.Config) {
	reloaded := cacheEmptyTargets(newCfg)
	for name, target := range startup {
		if reloaded[name] == target {
			continue
		}
		log.Warn().
			Str("proxy_name", name).
			Msg("cache empty proxy endpoint or timeout changed on reload, the change takes effect after a restart")
	}
}

// cacheEmptyReloadGuard holds the cache empty layout and targets of the startup config, and checks each
// reloaded config against them.
type cacheEmptyReloadGuard struct {
	layout  map[string]string
	targets map[string]cacheEmptyTarget
}

// newCacheEmptyReloadGuard takes the cache empty layout and targets of the startup config.
func newCacheEmptyReloadGuard(cfg config.Config) *cacheEmptyReloadGuard {
	return &cacheEmptyReloadGuard{
		layout:  cacheEmptyLayout(cfg),
		targets: cacheEmptyTargets(cfg),
	}
}

// check rejects newCfg when it changes the cache empty layout of the startup config.
func (g *cacheEmptyReloadGuard) check(newCfg config.Config) error {
	return checkCacheEmptyReload(g.layout, newCfg)
}

// warn logs each cache empty proxy whose endpoint or timeout changed in newCfg.
func (g *cacheEmptyReloadGuard) warn(newCfg config.Config) {
	warnCacheEmptyTargetsReload(g.targets, newCfg)
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
