package config

import (
	"fmt"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"

	"github.com/rs/zerolog/log"
)

var knownClientAddressUntrustedPeers = []string{"reject", "ignore"}

// validateClientAddress checks client_address. A disabled section is not checked, except that PROXY
// protocol needs the section enabled. An enabled section is checked with the same parser the server uses.
func validateClientAddress(c Config) error {
	cfg := c.ClientAddress
	if !cfg.Enabled {
		if cfg.ProxyProtocol.Enabled {
			return fmt.Errorf("client_address.proxy_protocol.enabled requires client_address.enabled")
		}
		return nil
	}
	if _, err := clientaddr.New(cfg); err != nil {
		return fmt.Errorf("client_address: %w", err)
	}
	if !cfg.ProxyProtocol.Enabled {
		return nil
	}
	if len(cfg.TrustedProxies) == 0 {
		return fmt.Errorf("client_address.proxy_protocol.enabled requires a non-empty client_address.trusted_proxies")
	}
	if !slices.Contains(knownClientAddressUntrustedPeers, cfg.ProxyProtocol.UntrustedPeers) {
		return fmt.Errorf("unknown client_address.proxy_protocol.untrusted_peers: \"%s\"", cfg.ProxyProtocol.UntrustedPeers)
	}
	if cfg.ProxyProtocol.ReadHeaderTimeout.ToDuration() <= 0 {
		return fmt.Errorf("client_address.proxy_protocol.read_header_timeout must be greater than zero")
	}
	if c.HTTP.InternalPort == "" {
		log.Warn().Msg("client_address.proxy_protocol is enabled and http_server.internal_port is empty: the PROXY protocol header is also required on the internal HTTP port")
	}
	return nil
}
