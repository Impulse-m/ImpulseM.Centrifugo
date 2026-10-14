package clientaddr

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Resolve returns the client address of a request. remoteAddr is the address of the connection,
// which is the PROXY protocol source when that protocol is in use. Headers are read only when
// remoteAddr is a trusted proxy. The zero Addr means the address is unknown.
//
// Behind a trusted proxy the header values are walked from right to left. Entries inside
// trusted_proxies are skipped, and the first entry which is not trusted is the client. Walking
// right to left and skipping trusted hops keeps the client address stable when several trusted
// proxies sit in a row. An entry which is not an address stops the walk and the peer is the
// result, because a trusted proxy never writes junk, so junk comes from the client. When every
// entry is trusted, or the header is missing, the peer is the result.
func (r *Resolver) Resolve(remoteAddr string, h http.Header) netip.Addr {
	peer, ok := parseAddr(remoteAddr)
	if !ok {
		return netip.Addr{}
	}
	if !r.isTrusted(peer) {
		return peer
	}
	values := h.Values(r.header)
	for v := len(values) - 1; v >= 0; v-- {
		entries := strings.Split(values[v], ",")
		for e := len(entries) - 1; e >= 0; e-- {
			addr, ok := parseAddr(entries[e])
			if !ok {
				return peer
			}
			if !r.isTrusted(addr) {
				return addr
			}
		}
	}
	return peer
}

// parseAddr parses an address, optionally with a port: "1.2.3.4", "1.2.3.4:5678",
// "[2001:db8::1]:443". The result is unmapped and has no zone, so an IPv4-mapped IPv6 address
// equals its IPv4 form.
func parseAddr(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	addr, err := netip.ParseAddr(s)
	if err != nil {
		host, _, splitErr := net.SplitHostPort(s)
		if splitErr != nil {
			return netip.Addr{}, false
		}
		addr, err = netip.ParseAddr(host)
		if err != nil {
			return netip.Addr{}, false
		}
	}
	return addr.WithZone("").Unmap(), true
}
