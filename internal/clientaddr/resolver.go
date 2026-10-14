// Package clientaddr resolves the address of the client behind trusted proxies. It reads the
// forwarded header of a trusted proxy and the PROXY protocol header of a client-facing port, and
// stores the result in the request or connection context.
package clientaddr

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// Resolver resolves client addresses with one trusted-proxy list and one forwarded header name.
type Resolver struct {
	header  string
	trusted []netip.Prefix
	ranges  []string
}

// New builds a Resolver from cfg. It fails on a trusted proxy which is neither an address nor a
// CIDR range, and on a header which is not a header name.
func New(cfg configtypes.ClientAddress) (*Resolver, error) {
	header := strings.ToLower(cfg.Header)
	if !isHeaderName(header) {
		return nil, fmt.Errorf("invalid header %q: must be a header name", cfg.Header)
	}
	trusted := make([]netip.Prefix, 0, len(cfg.TrustedProxies))
	ranges := make([]string, 0, len(cfg.TrustedProxies))
	for i, entry := range cfg.TrustedProxies {
		prefix, err := parseTrusted(entry)
		if err != nil {
			return nil, fmt.Errorf("trusted_proxies[%d]: %w", i, err)
		}
		trusted = append(trusted, prefix)
		ranges = append(ranges, prefix.String())
	}
	return &Resolver{
		header:  header,
		trusted: trusted,
		ranges:  ranges,
	}, nil
}

// parseTrusted parses an address or a CIDR range. A bare address becomes a single-host range. An
// IPv4-mapped IPv6 range is converted to the IPv4 range it stands for.
func parseTrusted(entry string) (netip.Prefix, error) {
	if strings.Contains(entry, "/") {
		prefix, err := netip.ParsePrefix(entry)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("invalid CIDR range %q", entry)
		}
		return normalizePrefix(prefix)
	}
	addr, err := netip.ParseAddr(entry)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("invalid address or CIDR range %q", entry)
	}
	return normalizePrefix(netip.PrefixFrom(addr, addr.BitLen()))
}

func normalizePrefix(prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.Addr().Is4In6() {
		return prefix.Masked(), nil
	}
	if prefix.Bits() < 96 {
		return netip.Prefix{}, fmt.Errorf("IPv4-mapped IPv6 range %q is too wide", prefix.String())
	}
	return netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96).Masked(), nil
}

// isTrusted reports whether addr is inside one of the trusted ranges. addr must be unmapped.
func (r *Resolver) isTrusted(addr netip.Addr) bool {
	for _, prefix := range r.trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// isHeaderName reports whether name, already lower-case, is a valid HTTP header field name
// (an RFC 9110 token).
func isHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return false
		}
	}
	return true
}

func isTokenChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		return true
	case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		return true
	}
	return false
}
