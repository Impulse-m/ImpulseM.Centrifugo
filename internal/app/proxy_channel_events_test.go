package app

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/client"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"

	"github.com/stretchr/testify/require"
)

func newChannelEventsTestProxy(endpoint string) configtypes.Proxy {
	return configtypes.Proxy{
		Endpoint: endpoint,
		Timeout:  configtypes.Duration(time.Second),
	}
}

func newChannelEventsTestProxyMap() *client.ProxyMap {
	return &client.ProxyMap{
		CacheEmptyProxies: map[string]proxy.CacheEmptyProxy{},
	}
}

// TestResolveCacheEmptyProxy checks the config picked for the default name and for the named proxies, and that
// the proxy of a name is created once.
func TestResolveCacheEmptyProxy(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Channel.Proxy.CacheEmpty = newChannelEventsTestProxy("http://localhost:8000/default")
	namedProxies := map[string]configtypes.Proxy{
		"other": newChannelEventsTestProxy("http://localhost:8000/other"),
	}

	t.Run("default", func(t *testing.T) {
		proxyMap := newChannelEventsTestProxyMap()
		p, err := resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, config.DefaultProxyName)
		require.NoError(t, err)
		require.Equal(t, cfg.Channel.Proxy.CacheEmpty, p)
		require.Contains(t, proxyMap.CacheEmptyProxies, config.DefaultProxyName)
	})

	t.Run("named", func(t *testing.T) {
		proxyMap := newChannelEventsTestProxyMap()
		p, err := resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, "other")
		require.NoError(t, err)
		require.Equal(t, namedProxies["other"], p)
		require.Contains(t, proxyMap.CacheEmptyProxies, "other")
		require.NotContains(t, proxyMap.CacheEmptyProxies, config.DefaultProxyName)
	})

	t.Run("not found", func(t *testing.T) {
		proxyMap := newChannelEventsTestProxyMap()
		_, err := resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, "missing")
		require.Error(t, err)
		require.Empty(t, proxyMap.CacheEmptyProxies)
	})

	t.Run("reuse", func(t *testing.T) {
		proxyMap := newChannelEventsTestProxyMap()
		_, err := resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, "other")
		require.NoError(t, err)
		before := proxyMap.CacheEmptyProxies["other"]

		_, err = resolveCacheEmptyProxy(proxyMap, cfg, namedProxies, "other")
		require.NoError(t, err)
		require.Len(t, proxyMap.CacheEmptyProxies, 1)
		require.Same(t, before, proxyMap.CacheEmptyProxies["other"])
	})
}

// TestEnableCacheEmptyProxies checks which cache empty proxies are created for channels without namespace and
// for the namespaces: only the enabled ones, each under the name it refers to.
func TestEnableCacheEmptyProxies(t *testing.T) {
	namedProxies := map[string]configtypes.Proxy{
		"other": newChannelEventsTestProxy("http://localhost:8000/other"),
	}

	testCases := []struct {
		name     string
		modify   func(cfg *config.Config)
		wantKeys []string
		wantErr  bool
	}{
		{
			name:     "nothing enabled",
			modify:   func(cfg *config.Config) {},
			wantKeys: nil,
		},
		{
			name: "without namespace",
			modify: func(cfg *config.Config) {
				cfg.Channel.WithoutNamespace.CacheEmptyProxyEnabled = true
				cfg.Channel.WithoutNamespace.CacheEmptyProxyName = config.DefaultProxyName
			},
			wantKeys: []string{config.DefaultProxyName},
		},
		{
			name: "enabled namespace only",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
					{Name: "chat", ChannelOptions: configtypes.ChannelOptions{
						CacheEmptyProxyEnabled: true,
						CacheEmptyProxyName:    "other",
					}},
					{Name: "news", ChannelOptions: configtypes.ChannelOptions{
						CacheEmptyProxyName: "missing",
					}},
				}
			},
			wantKeys: []string{"other"},
		},
		{
			name: "named proxy not found",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
					{Name: "chat", ChannelOptions: configtypes.ChannelOptions{
						CacheEmptyProxyEnabled: true,
						CacheEmptyProxyName:    "missing",
					}},
				}
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Channel.Proxy.CacheEmpty = newChannelEventsTestProxy("http://localhost:8000/default")
			tc.modify(&cfg)

			proxyMap := newChannelEventsTestProxyMap()
			err := enableCacheEmptyProxies(proxyMap, cfg, namedProxies)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			keys := make([]string, 0, len(proxyMap.CacheEmptyProxies))
			for name := range proxyMap.CacheEmptyProxies {
				keys = append(keys, name)
			}
			require.ElementsMatch(t, tc.wantKeys, keys)
		})
	}
}

// newCacheEmptyStartupConfig returns the config at startup for TestCheckCacheEmptyReload: the chat namespace
// has its cache empty proxy enabled, under the name other.
func newCacheEmptyStartupConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Channel.Proxy.CacheEmpty = newChannelEventsTestProxy("http://localhost:8000/default")
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
		{Name: "chat", ChannelOptions: configtypes.ChannelOptions{
			CacheEmptyProxyEnabled: true,
			CacheEmptyProxyName:    "other",
		}},
	}
	return cfg
}

// TestCheckCacheEmptyReload checks that a reload keeps the cache empty layout of the startup config: the proxy
// of each enabled channel group, and which groups are enabled. Options of a proxy may change on reload.
func TestCheckCacheEmptyReload(t *testing.T) {
	startup := cacheEmptyLayout(newCacheEmptyStartupConfig())

	testCases := []struct {
		name    string
		modify  func(cfg *config.Config)
		wantErr bool
	}{
		{
			name:   "unchanged",
			modify: func(cfg *config.Config) {},
		},
		{
			name: "endpoint of a cache empty proxy changed",
			modify: func(cfg *config.Config) {
				cfg.Channel.Proxy.CacheEmpty = newChannelEventsTestProxy("http://localhost:9000/default")
			},
		},
		{
			name: "namespace disabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces[0].CacheEmptyProxyEnabled = false
			},
			wantErr: true,
		},
		{
			name: "proxy of namespace renamed",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces[0].CacheEmptyProxyName = config.DefaultProxyName
			},
			wantErr: true,
		},
		{
			name: "namespace removed",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = nil
			},
			wantErr: true,
		},
		{
			name: "namespace enabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{
					Name: "news",
					ChannelOptions: configtypes.ChannelOptions{
						CacheEmptyProxyEnabled: true,
						CacheEmptyProxyName:    "other",
					},
				})
			},
			wantErr: true,
		},
		{
			name: "disabled namespace added",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{
					Name: "news",
					ChannelOptions: configtypes.ChannelOptions{
						CacheEmptyProxyName: "missing",
					},
				})
			},
		},
		{
			name: "without namespace enabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.WithoutNamespace.CacheEmptyProxyEnabled = true
				cfg.Channel.WithoutNamespace.CacheEmptyProxyName = config.DefaultProxyName
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newCacheEmptyStartupConfig()
			tc.modify(&cfg)
			err := checkCacheEmptyReload(startup, cfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
