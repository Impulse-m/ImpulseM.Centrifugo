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

// buildChannelStateProxy creates the channel state proxy of channel.proxy.state. The proxy is shared by
// all namespaces: one endpoint receives the events of all of them.
func buildChannelStateProxy(cfg config.Config) (proxy.ChannelStateProxy, error) {
	csp, err := proxy.GetChannelStateProxy("state", cfg.Channel.Proxy.State)
	if err != nil {
		return nil, fmt.Errorf("error creating channel state proxy: %w", err)
	}
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
	if len(channelStateLayout(cfg)) > 0 {
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
	for _, group := range cacheEmptyGroups(cfg) {
		err := enableCacheEmptyProxy(proxyMap, cfg, namedProxies, group.name, group.namespace)
		if err != nil {
			return err
		}
	}
	return nil
}

// cacheEmptyGroup is a channel group with a cache empty proxy enabled: the name of the proxy and the namespace
// of the group. The group of channels without namespace has the empty namespace.
type cacheEmptyGroup struct {
	name      string
	namespace string
}

// cacheEmptyGroups returns the channel groups with a cache empty proxy enabled: the channels without namespace
// first, then the namespaces in config order.
func cacheEmptyGroups(cfg config.Config) []cacheEmptyGroup {
	var groups []cacheEmptyGroup
	withoutNamespace := cfg.Channel.WithoutNamespace
	if withoutNamespace.CacheEmptyProxyEnabled {
		groups = append(groups, cacheEmptyGroup{name: withoutNamespace.CacheEmptyProxyName, namespace: ""})
	}
	for _, ns := range cfg.Channel.Namespaces {
		if ns.CacheEmptyProxyEnabled {
			groups = append(groups, cacheEmptyGroup{name: ns.CacheEmptyProxyName, namespace: ns.Name})
		}
	}
	return groups
}

// cacheEmptyLayout returns the cache empty proxy name of each enabled channel group, keyed by namespace. Channels
// without namespace are keyed by the empty string.
func cacheEmptyLayout(cfg config.Config) map[string]string {
	layout := map[string]string{}
	for _, group := range cacheEmptyGroups(cfg) {
		layout[group.namespace] = group.name
	}
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

// proxyTarget is the endpoint and the timeout of a proxy, as the config sets them.
type proxyTarget struct {
	endpoint string
	timeout  configtypes.Duration
}

// newProxyTarget returns the target of the proxy p.
func newProxyTarget(p configtypes.Proxy) proxyTarget {
	return proxyTarget{endpoint: p.Endpoint, timeout: p.Timeout}
}

// cacheEmptyTargets returns the target of each cache empty proxy of the enabled channel groups, keyed by the
// proxy name. The proxies are created from these targets at startup.
func cacheEmptyTargets(cfg config.Config) map[string]proxyTarget {
	named := make(map[string]configtypes.Proxy, len(cfg.Proxies))
	for _, p := range cfg.Proxies {
		named[p.Name] = p.Proxy
	}
	targets := map[string]proxyTarget{}
	for _, group := range cacheEmptyGroups(cfg) {
		p, _ := cacheEmptyProxyConfig(cfg, named, group.name)
		targets[group.name] = newProxyTarget(p)
	}
	return targets
}

// warnCacheEmptyTargetsReload logs a warning for each cache empty proxy whose endpoint or timeout differs in the
// reloaded config. A reload keeps the proxies created at startup, so the new values take effect on restart.
func warnCacheEmptyTargetsReload(startup map[string]proxyTarget, newCfg config.Config) {
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

// reloadGuard holds the layouts and the targets of the startup config, and checks each reloaded config
// against them. The proxies and the engine hooks are created from the startup config, so a reload keeps
// their layout and only logs the changes which take effect after a restart.
type reloadGuard struct {
	cacheEmptyLayout      map[string]string
	cacheEmptyTargets     map[string]proxyTarget
	channelStateGroups    []string
	channelStateTarget    proxyTarget
	channelStatePlacement channelStatePlacement
}

// newReloadGuard takes the cache empty and channel state settings of the startup config.
func newReloadGuard(cfg config.Config) *reloadGuard {
	return &reloadGuard{
		cacheEmptyLayout:      cacheEmptyLayout(cfg),
		cacheEmptyTargets:     cacheEmptyTargets(cfg),
		channelStateGroups:    channelStateLayout(cfg),
		channelStateTarget:    channelStateTargetOf(cfg),
		channelStatePlacement: channelStatePlacementOf(cfg),
	}
}

// check rejects newCfg when it changes the cache empty layout or the channel state groups of the startup config.
func (g *reloadGuard) check(newCfg config.Config) error {
	if err := checkCacheEmptyReload(g.cacheEmptyLayout, newCfg); err != nil {
		return err
	}
	return checkChannelStateReload(g.channelStateGroups, newCfg)
}

// warn logs the cache empty and channel state settings of newCfg which changed and take effect after a restart.
// The channel state settings are logged only when channel state is enabled at startup.
func (g *reloadGuard) warn(newCfg config.Config) {
	warnCacheEmptyTargetsReload(g.cacheEmptyTargets, newCfg)
	if len(g.channelStateGroups) == 0 {
		return
	}
	if channelStateTargetOf(newCfg) != g.channelStateTarget {
		log.Warn().Msg("channel state proxy endpoint or timeout changed on reload, the change takes effect after a restart")
	}
	if channelStatePlacementOf(newCfg) != g.channelStatePlacement {
		log.Warn().Msg("channel state vacated_event_delay or num_partitions changed on reload, the change takes effect after a restart")
	}
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
