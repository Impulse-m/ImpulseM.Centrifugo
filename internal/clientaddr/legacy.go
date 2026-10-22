package clientaddr

import (
	"net"
	"net/http"
	"strings"
)

// legacyThrottleKey derives the client address used as the throttle key when client_address is
// disabled. It is kept as it was before the section existed, so disabling the feature keeps the
// upstream behaviour.
//
// Forwarded headers are only trusted when the immediate socket peer is a private
// or loopback address, i.e. a local reverse proxy or load balancer (Centrifugo is
// commonly deployed behind one). For a direct public client the headers are
// client-controlled and ignored.
//
// Behind such a proxy the last (rightmost) X-Forwarded-For entry is used: it is
// the address the proxy in front of Centrifugo appended for the connection it
// received, while entries further left can come from the client. With several
// proxies in a row this is the address of the previous proxy, so their clients
// share one key - coarse, but not something a client can choose. Proxies must
// append to (or overwrite) X-Forwarded-For rather than pass a client's value on
// unchanged. X-Real-IP is not used. A value which is not an address falls back
// to the peer, so junk cannot inflate map keys or fragment the keyspace.
func legacyThrottleKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil || !isInternalIP(peer) {
		return host
	}
	// Peer is a trusted local proxy: use the address it appended.
	values := r.Header.Values("X-Forwarded-For")
	if len(values) == 0 {
		return peer.String()
	}
	last := values[len(values)-1]
	if i := strings.LastIndexByte(last, ','); i >= 0 {
		last = last[i+1:]
	}
	if ip := parseForwardedIP(last); ip != nil {
		return ip.String()
	}
	return peer.String()
}

// legacyLogAddr is the address written to the request log when client_address is disabled: the
// X-Real-IP header, then X-Forwarded-For, then the remote address, all taken from any peer. Known gap: a client can
// choose the address it logs this way. It is kept until the trust rule of the request log is decided, because the
// default log must not change. The throttle key does not use these headers, see legacyThrottleKey.
func legacyLogAddr(r *http.Request) string {
	addr := r.Header.Get("X-Real-IP")
	if addr == "" {
		addr = r.Header.Get("X-Forwarded-For")
		if addr == "" {
			addr = r.RemoteAddr
		}
	}
	return addr
}

func isInternalIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate()
}

// parseForwardedIP parses one X-Forwarded-For entry: an address, optionally
// with a port ("1.2.3.4:5678", "[2001:db8::1]:443"). It returns nil for
// anything else.
func parseForwardedIP(s string) net.IP {
	s = strings.TrimSpace(s)
	if ip := net.ParseIP(s); ip != nil {
		return ip
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		return net.ParseIP(host)
	}
	return nil
}
