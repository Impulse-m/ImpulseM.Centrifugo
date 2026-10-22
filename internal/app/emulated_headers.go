package app

import (
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// emulatedHeaderNames returns the lower-case union of the emulated_headers names of every configured
// proxy. Namespaces reference either the default proxies or the named ones, so these are all of them.
// The set may hold names of proxies no namespace uses, which keeps a few more headers and never drops one.
func emulatedHeaderNames(cfg config.Config) map[string]struct{} {
	proxies := []configtypes.Proxy{
		cfg.Client.Proxy.Connect.Proxy,
		cfg.Client.Proxy.Refresh.Proxy,
		cfg.Channel.Proxy.Subscribe,
		cfg.Channel.Proxy.Publish,
		cfg.Channel.Proxy.SubRefresh,
		cfg.Channel.Proxy.SubscribeStream,
		cfg.Channel.Proxy.MapPublish,
		cfg.Channel.Proxy.MapRemove,
		cfg.Channel.Proxy.SharedPollRefresh,
		cfg.RPC.Proxy,
	}
	for _, named := range cfg.Proxies {
		proxies = append(proxies, named.Proxy)
	}
	names := make(map[string]struct{})
	for _, p := range proxies {
		for _, name := range p.EmulatedHeaders {
			names[strings.ToLower(name)] = struct{}{}
		}
	}
	return names
}
